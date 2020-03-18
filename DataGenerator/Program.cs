namespace DataGenerator
{
    using Microsoft.Azure.EventHubs;
    using System;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;

    public class Program
    {
        private static EventHubClient eventHubClient;
        private const string EventHubConnectionString = "EventHubConnectionString";
        private const string EventHubName = "EventHubName";

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

            eventHubClient = EventHubClient.CreateFromConnectionString(connectionStringBuilder.ToString());

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
                    await eventHubClient.SendAsync(new EventData(Encoding.UTF8.GetBytes(message)));
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
                    await eventHubClient.SendAsync(new EventData(Encoding.UTF8.GetBytes(message)));
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