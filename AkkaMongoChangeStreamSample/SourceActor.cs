using Akka.Actor;
using Akka.Persistence;
using Bogus;

namespace AkkaMongoChangeStreamSample;

public class SourceActor : ReceivePersistentActor, IWithTimers
{
    public SourceActor()
    {
        PersistenceId = "source-actor";
        Command<string>(s => Persist(new MyEvent(s), _ => { SendDelayedMessage(); }));
        SendDelayedMessage();
    }

    private void SendDelayedMessage()
    {
        Timers.StartSingleTimer(PersistenceId, Faker.Lorem.Sentence(), TimeSpan.FromMilliseconds(500));
    }

    public ITimerScheduler Timers { get; set; } = null!;
    public override string PersistenceId { get; }

    private Faker Faker { get; } = new Faker();
}


internal record MyEvent(string Data);