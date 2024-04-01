using Akka.Actor;
using Akka.Hosting;
using Akka.Persistence.MongoDb.Hosting;
using Akka.Persistence.MongoDb.Journal;
using AkkaMongoChangeStreamSample;
using Microsoft.Extensions.Hosting;
using MongoDB.Driver;

// Start up an in-memory MongoDB single node replica set instance so we can test the change stream
using var mongo = EphemeralMongo.MongoRunner.Run(new EphemeralMongo.MongoRunnerOptions { UseSingleNodeReplicaSet = true, KillMongoProcessesWhenCurrentProcessExits = true });
var cs = mongo.ConnectionString;
var withDb = cs.Substring(0, cs.LastIndexOf('/') + 1) + "AkkaPersistence" + cs.Substring(cs.LastIndexOf('/') + 1);

var builder = Host.CreateApplicationBuilder(args);

var journalOptions = new MongoDbJournalOptions() { ConnectionString = withDb, AutoInitialize = true, LegacySerialization = false };

builder.Services.AddAkka("MyActorSystem", configurationBuilder =>
{
    configurationBuilder
        .WithActors((system, _) =>
        {
            system.ActorOf<SourceActor>("source");
            system.ActorOf(Props.Create<ConsumerActor>(new MongoClient(withDb).GetDatabase("AkkaPersistence").GetCollection<JournalEntry>("EventJournal")), "consumer");
        })
        .WithMongoDbPersistence(
            new MongoDbJournalOptions() { ConnectionString = withDb, AutoInitialize = true, LegacySerialization = false },
            new MongoDbSnapshotOptions() { ConnectionString = withDb });
});

var app = builder.Build();

Console.WriteLine("****** Mongo is available at {0}", withDb);
app.Run();
