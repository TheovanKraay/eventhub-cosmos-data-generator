using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;

namespace FunctionRegion1
{
    public static class Function1
    {

        private static readonly string _endpointUrl = System.Environment.GetEnvironmentVariable("endpointUrl");
        private static Uri accountEndPoint = new Uri(_endpointUrl);
        private static readonly string _primaryKey = System.Environment.GetEnvironmentVariable("primaryKey");
        private static readonly string _databaseId = "database";
        private static readonly string _containerId = "asa";
        private static ConnectionPolicy connectionPolicy = new ConnectionPolicy();
        private static CosmosClient cosmosClient = new CosmosClient(_endpointUrl, _primaryKey, 
            new CosmosClientOptions() {ApplicationRegion = Regions.UKSouth,}); //set preferred region

        //req coming from stream analytics would be in something like this format:

        //[{"id":"1","pk": "uksouth"},
        //{"id":"2","pk": "uksouth"}]

        [FunctionName("Function1")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = null)] HttpRequest req,
            ILogger log)
        {
            log.LogInformation("C# HTTP trigger function processed a request.");

            string requestBody = await new StreamReader(req.Body).ReadToEndAsync();

            var messages = JsonConvert.DeserializeObject<List<object>>(requestBody);

            var container = cosmosClient.GetContainer(_databaseId, _containerId);
            foreach (var message in messages)
            {
                await container.CreateItemAsync(message);
            }
                
            Console.WriteLine("requestBody: " + requestBody);

            dynamic data = JsonConvert.DeserializeObject(requestBody);

            return messages != null
                ? (ActionResult)new OkObjectResult($"Hello, {messages}")
                : new BadRequestObjectResult("Please pass a name on the query string or in the request body");

        }
    }
}
