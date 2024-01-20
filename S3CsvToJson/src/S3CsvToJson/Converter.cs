using System.Globalization;
using System.Text;
using Amazon;
using Amazon.S3;
using Amazon.S3.Model;
using CsvHelper;
using Newtonsoft.Json;

namespace S3CsvToJson;

public class Converter
{
    private IAmazonS3 _s3Client;

    private RegionEndpoint _region = null;

    public Converter(string Region)
    {
        // Set the AWS region where your S3 bucket is located
        _region = RegionEndpoint.GetBySystemName(Region);

        // Create an S3 client
        _s3Client = new AmazonS3Client(_region);

    }

    public void Run(Request Request)
    {
        if (Request.Source.EndsWith(".csv"))
        {
            //https://github.com/JoshClose/CsvHelper/issues/1259

            string content = GetFileContentFromS3(Request.Bucketname, Request.Source).Result;

            // Parse CSV content
            using (StringReader reader = new StringReader(content))
            using (var csv = new CsvReader(reader, CultureInfo.InvariantCulture))
            {
                csv.Read();
                csv.ReadHeader();
                var records = csv.GetRecords<dynamic>().ToList();

                var json = JsonConvert.SerializeObject(records);

                //Save
                /*convert string to stream*/
                byte[] byteArray = Encoding.ASCII.GetBytes(json);
                MemoryStream stream = new MemoryStream(byteArray);

                string resultPath = "";

                if (!string.IsNullOrEmpty(Request.ResultPrefix)) 
                {
                    string filename = Request.Source.Split("/").Last();
                    resultPath = Request.ResultPrefix + filename.Replace(".csv",".json");
                }
                else 
                {
                    resultPath = Request.Result;
                }
                
                SaveFile(_s3Client, Request.Bucketname, resultPath, stream, "application/json");
            }
        }
    }

    public void SaveFile(IAmazonS3 _s3Client, string Bucketname,
    string S3Path, Stream Stream, string ContentType)
    {
        var putRequest = new PutObjectRequest
        {
            BucketName = Bucketname,
            Key = S3Path,
            ContentType = ContentType,
            InputStream = Stream
        };

        _s3Client.PutObjectAsync(putRequest).Wait();
    }

    private async Task<string> GetFileContentFromS3(string bucketName, string key)
    {
        try
        {
            GetObjectRequest request = new GetObjectRequest
            {
                BucketName = bucketName,
                Key = key
            };

            using (GetObjectResponse response = await _s3Client.GetObjectAsync(request))
            using (Stream responseStream = response.ResponseStream)
            using (StreamReader reader = new StreamReader(responseStream))
            {
                return await reader.ReadToEndAsync();
            }
        }
        catch (AmazonS3Exception e)
        {
            // Handle S3 exception
            return $"Error getting template: {e.Message}";
        }
    }
}