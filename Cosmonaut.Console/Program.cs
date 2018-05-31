﻿using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Cosmonaut.Extensions;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Microsoft.Extensions.DependencyInjection;

namespace Cosmonaut.Console
{
    class Program
    {
        static async Task Main(string[] args)
        {
            var connectionPolicy = new ConnectionPolicy
            {
                ConnectionProtocol = Protocol.Https,
                ConnectionMode = ConnectionMode.Gateway
            };

            var cosmosSettings = new CosmosStoreSettings("localtest", 
                "https://localhost:8081", 
                "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw=="
                , connectionPolicy
                , defaultCollectionThroughput: 5000);
           
            var serviceCollection = new ServiceCollection();
            serviceCollection.AddCosmosStore<Book>(cosmosSettings);
            serviceCollection.AddCosmosStore<Car>(cosmosSettings);

            var provider = serviceCollection.BuildServiceProvider();

            var booksStore = provider.GetService<ICosmosStore<Book>>();
            var carStore = provider.GetService<ICosmosStore<Car>>();


            var booksRemoved = booksStore.RemoveAsync(x => true).GetAwaiter().GetResult();
            var carsRemoved = carStore.RemoveAsync(x => true).GetAwaiter().GetResult();
            System.Console.WriteLine($"Started");
            
            var books = new List<Book>();
            for (int i = 0; i < 50; i++)
            {
                books.Add(new Book
                {
                    Id = Guid.NewGuid().ToString(),
                    Name = "Test " + i,
                    AnotherRandomProp = "Random " + i
                });
            }

            var cars = new List<Car>();
            for (int i = 0; i < 50; i++)
            {
                cars.Add(new Car
                {
                    Id = Guid.NewGuid().ToString(),
                    ModelName = "Car " + i,
                });
            }

            var watch = new Stopwatch();
            watch.Start();
            var addedBooks = booksStore.AddRangeAsync(books).Result;
            var addedCars = carStore.AddRangeAsync(cars).Result;
            System.Console.WriteLine($"Added 100 documents in {watch.ElapsedMilliseconds}ms");
            watch.Restart();
            //await Task.Delay(3000);

            var addedRetrieved = booksStore.Query().ToListAsync().Result;
            System.Console.WriteLine($"Retrieved 50 documents in {watch.ElapsedMilliseconds}ms");
            watch.Restart();
            foreach (var addedre in addedRetrieved)
            {
                addedre.AnotherRandomProp += " Nick";
            }

            var updated = booksStore.UpsertRangeAsync(addedRetrieved).Result;
            System.Console.WriteLine($"Updated 50 documents in {watch.ElapsedMilliseconds}ms");
            watch.Restart();

            var removed = booksStore.RemoveRangeAsync(addedRetrieved).Result;
            System.Console.WriteLine($"Removed 50 documents in {watch.ElapsedMilliseconds}ms");
            watch.Reset();
            watch.Stop();
            System.Console.ReadKey();
        }
    }
}
