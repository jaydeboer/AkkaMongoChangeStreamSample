using System.Reactive.Linq;
using Akka.Actor;
using Akka.Event;
using Akka.Persistence;
using Akka.Persistence.MongoDb.Journal;
using Akka.Persistence.MongoDb.Query;
using Akka.Persistence.Query;
using Akka.Serialization;
using Akka.Streams;
using Akka.Streams.Dsl;
using MongoDB.Bson;
using MongoDB.Driver;

namespace AkkaMongoChangeStreamSample;

public class ConsumerActor : ReceivePersistentActor, IWithTimers
{
    public ConsumerActor(IMongoCollection<JournalEntry> journalCollection)
    {
        _journalCollection = journalCollection;
        _serializer = Context.System.Serialization.FindSerializerForType(typeof(Persistent));
        Command<StartConsuming>(_ => Consume());
        Command<EventEnvelope>(ee => ee.Event is MyEvent, ee => Console.WriteLine($"ConsumerActor received: {(ee.Event as MyEvent)?.Data}"));

        Timers.StartSingleTimer("key", StartConsuming.Instance, TimeSpan.FromSeconds(3));
    }
    public ITimerScheduler Timers { get; set; } = null!;
    public override string PersistenceId { get; } = "consumer-actor";

    public override void AroundPostStop()
    {
        if (_cts.IsCancellationRequested == false)
            _cts.Cancel();

        base.AroundPostStop();
    }

    private readonly ILoggingAdapter _log = Context.GetLogger();

    private void Consume()
    {
        var readJournal = Context.System.ReadJournalFor<MongoDbReadJournal>(MongoDbReadJournal.Identifier);
        var materializer = Context.Materializer();
        _self = Self;
        var eventsByTag = readJournal.CurrentEventsByPersistenceId("source-actor", 0, long.MaxValue);

        var observable = Observable.Create<EventEnvelope>(async (obs, token) =>
        {
            var pipeline = new EmptyPipelineDefinition<ChangeStreamDocument<JournalEntry>>()
                .Match(change => change.FullDocument.PersistenceId == "source-actor");

            using var cursor = _journalCollection.Watch(pipeline, cancellationToken: token);

            while (await cursor.MoveNextAsync(token))
            {
                foreach (var change in cursor.Current)
                {
                    var persistent = ToPersistenceRepresentation(change.FullDocument);
                    var ee = new EventEnvelope(Offset.Sequence(change.FullDocument.Ordering.Value),
                        persistent.PersistenceId, persistent.SequenceNr, persistent.Payload, persistent.Timestamp,
                        change.FullDocument.Tags?.ToArray() ?? []);
                    obs.OnNext(ee);
                }
            }
        });

        var changeStreamSource = Source.FromObservable(observable);
        eventsByTag.Concat(changeStreamSource).RunWith(Sink.ActorRef<EventEnvelope>(
                _self,
                ExpectedEndOfStreamEvent.Instance,
                ex =>
                {
                    _log.Info(ex.Message);
                    return ex;
                }), materializer);
    }

    private static readonly BsonTimestamp ZeroTimestamp = new(0);
    private readonly Serializer _serializer;
    private readonly CancellationTokenSource _cts = new();
    private IActorRef? _self;
    private IMongoCollection<JournalEntry> _journalCollection;


    // The following methods were lifted from the Akka source code
    private Persistent ToPersistenceRepresentation(JournalEntry entry)
    {
        var bytes = (byte[])entry.Payload;
        var output = _serializer.FromBinary<Persistent>(bytes);

        // backwards compatibility for https://github.com/akkadotnet/akka.net/pull/4680
        // it the timestamp is not defined in the binary payload
        if (output.Timestamp == 0L)
        {
            output = (Persistent)output.WithTimestamp(ToTicks(entry.Ordering));
        }
        return output;
    }

    private static long ToTicks(BsonTimestamp? bson)
    {
        // BSON Timestamps are stored natively as Unix epoch seconds + an ordinal value

        // need to use BsonTimestamp.Timestamp because the ordinal value doesn't actually have any
        // bearing on the time - it's used to try to somewhat order the events that all occurred concurrently
        // according to the MongoDb clock. No need to include that data in the EventEnvelope.Timestamp field
        // which is used entirely for end-user purposes.
        //
        // See https://docs.mongodb.com/manual/reference/bson-types/#timestamps
        bson ??= ZeroTimestamp;
        return DateTimeOffset.FromUnixTimeSeconds(bson.Timestamp).Ticks;
    }
}

internal record StartConsuming
{
    private StartConsuming() { }
    public static StartConsuming Instance { get; } = new();
}

internal record ExpectedEndOfStreamEvent
{
    private ExpectedEndOfStreamEvent() { }
    public static ExpectedEndOfStreamEvent Instance { get; } = new();
};
