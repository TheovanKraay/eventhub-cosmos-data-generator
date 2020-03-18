namespace DataGenerator
{
    using Microsoft.Azure.EventHubs;
    using System;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;

    public class Program
    {
        private static EventHubClient eventHubClient1;
        private static EventHubClient eventHubClient2;
        private const string EventHubConnectionString = "EventHubConnectionString";
        private const string EventHubName = "eventhub";

        private const string EventHubConnectionString2 = "EventHubConnectionString2";
        private const string EventHubName2 = "eventhub";

        
        public static void Main(string[] args)
        {
            MainAsync(args).GetAwaiter().GetResult();
        }

        private static async Task MainAsync(string[] args)
        {
            // Creates an EventHubsConnectionStringBuilder object from the connection string, and sets the EntityPath.
            // Typically, the connection string should have the entity path in it, but for the sake of this simple scenario
            // we are using the connection string from the namespace.
            var connectionStringBuilder = new EventHubsConnectionStringBuilder(EventHubConnectionString)
            {
                EntityPath = EventHubName
            };

            eventHubClient1 = EventHubClient.CreateFromConnectionString(connectionStringBuilder.ToString());

            var connectionStringBuilder2 = new EventHubsConnectionStringBuilder(EventHubConnectionString2)
            {
                EntityPath = EventHubName2
            };

            eventHubClient2 = EventHubClient.CreateFromConnectionString(connectionStringBuilder2.ToString());

            //send to the event hubs in parallel

            Thread t1 = new Thread(async () =>
            {
                await SendMessagesToEventHub1(1000);
            });

            Thread t2 = new Thread(async () =>
            {
                await SendMessagesToEventHub2(1000);
            });

            t1.Start();
            t2.Start();

            //await eventHubClient.CloseAsync();

            Console.WriteLine("Press ENTER to exit.");
            Console.ReadLine();
        }

        // Uses the event hub client to send 100 messages to the event hub.
        private static async Task SendMessagesToEventHub1(int numMessagesToSend)
        {
            for (var i = 0; i < numMessagesToSend; i++)
            {
                try
                {
                    Guid id = Guid.NewGuid();
                    //var message = $"Message {i}";
                    var message = "{\"id\":\""+ id + "\",\"pk\": \"uksouth\"}";
                    Console.WriteLine($"Sending message: {message}");
                    await eventHubClient1.SendAsync(new EventData(Encoding.UTF8.GetBytes(message)));
                }
                catch (Exception exception)
                {
                    Console.WriteLine($"{DateTime.Now} > Exception: {exception.Message}");
                }

                await Task.Delay(1000);
            }

            Console.WriteLine($"{numMessagesToSend} messages sent.");
        }

        private static async Task SendMessagesToEventHub2(int numMessagesToSend)
        {
            for (var i = 0; i < numMessagesToSend; i++)
            {
                try
                {
                    Guid id = Guid.NewGuid();
                    //var message = $"Message {i}";
                    var message = "{\"id\":\"" + id + "\",\"pk\": \"ukwest\"}";
                    Console.WriteLine($"Sending message: {message}");
                    await eventHubClient2.SendAsync(new EventData(Encoding.UTF8.GetBytes(message)));
                }
                catch (Exception exception)
                {
                    Console.WriteLine($"{DateTime.Now} > Exception: {exception.Message}");
                }

                await Task.Delay(1000);
            }

            Console.WriteLine($"{numMessagesToSend} messages sent.");
        }
    }
}