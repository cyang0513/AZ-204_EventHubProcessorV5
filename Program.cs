using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Xml.Schema;
using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Processor;
using Azure.Storage.Blobs;

namespace EventHubProcessorV5
{
   class Program
   {
      static async Task Main(string[] args)
      {
         var storageConn = "DefaultEndpointsProtocol=https;AccountName=chyastorage;AccountKey=uzMzvbJTKwrvFlwcnRQJrFUal6D4tz2YIgqJvFCDurFJuiGxGr/OHJtPdgbdkXjIa3O8YWKb2yzotRHfJGlQig==;EndpointSuffix=core.windows.net";
         var blobContainer = "event";
         var hubConn = "Endpoint=sb://chyaeventhub.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=//JboujWToYmJ2tytgSKApsCgERt2OC4O4/H5uXD7CQ=";
         var consumerGroup = "$Default";
         var hubName = "chyahub";

         Console.WriteLine("Event Hub Processor V5...");

         var blobClient = new BlobContainerClient(storageConn, blobContainer);
         var client = new EventProcessorClient(blobClient, consumerGroup, hubConn, hubName);

         var token = new CancellationTokenSource().Token;

         client.ProcessEventAsync += ProcessEvent;
         client.ProcessErrorAsync += ProcessError;

         try
         {
            Console.WriteLine("Start processing, press ENTER to stop...");
            await client.StartProcessingAsync(token);

            Console.ReadLine();

         }
         catch (Exception e)
         {
            Console.WriteLine(e);
         }
         finally
         {
            await client.StopProcessingAsync(token);
            client.ProcessEventAsync -= ProcessEvent;
            client.ProcessErrorAsync -= ProcessError;
         }


      }

      static Task ProcessError(ProcessErrorEventArgs arg)
      {
         Console.WriteLine($"Error: {arg.Exception.Message} at partition: {arg.PartitionId}");
         return Task.CompletedTask;
      }

      static Task ProcessEvent(ProcessEventArgs arg)
      {
         if (arg.CancellationToken.IsCancellationRequested)
         {
            return Task.CompletedTask;
         }

         var data = Encoding.ASCII.GetString(arg.Data.EventBody.ToArray());

         Console.WriteLine(data + " Partition: " + arg.Partition.PartitionId);
         

         return Task.CompletedTask;
      }
   }
}
