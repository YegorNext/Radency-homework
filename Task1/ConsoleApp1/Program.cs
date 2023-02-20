using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Xml.Linq;
using System.Web;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Windows.Markup;
using System.Collections.Concurrent;
using System.Threading;
using System.Timers;

namespace ConsoleApp1
{
    public class Config
    {
        public string folder_a_path { get; set; }
        public string folder_b_path { get;set; }

    }

    public class TransactionData
    {
        public string city { get; set; }
        public IEnumerable<Services> services { get; set; }
        public decimal total { get; set; }
    }
    public class Services
    {
        public string name { get; set; }
        public IEnumerable<Payers> payers{ get; set; }

        public decimal total { get; set; }
    }
    public class Payers
    {
        public string name { get; set; }
        public decimal payment { get; set; }
        public string date { get; set; }
        public long account_number { get; set; }
    }
    public static class ConcurrencyProperties
    {
        public const int tasksNum = 3;
        public static int ErrorCounter = 0;
        public static int procLineCounter = 0;
        public static int procFileCounter = 0;
        public static int fileNumber = 1;
        public static ConcurrentQueue<string> filepathes = new ConcurrentQueue<string>();
        public static ConcurrentBag<string> validLines = new ConcurrentBag<string>();
        public static ConcurrentBag<string> invalidFilesPath = new ConcurrentBag<string>();

    }
    internal class Program
    {
        private static Config jsonData = null;
        private static System.Timers.Timer logTimer;
        static void Main(string[] args)
        {
            string jsonDataPath = "D:\\Student\\Radency\\Task1\\ConsoleApp1\\Task1\\ConsoleApp1\\config.json";
            string command = "";
            try
            {
                jsonData = JsonSerializer.Deserialize<Config>(System.IO.File.ReadAllText(jsonDataPath));
            }
            catch(Exception e) 
            {
                Console.WriteLine("INFO/ERROR: Can't read json data file, {0}", e.ToString());
                return;
            }

            
            Console.WriteLine("########Welcome to JSON Creator v3.0########\nPlease, choose an action (start & reset & stop)");
            while(command != "start" && command != "stop")
            {
                command = Console.ReadLine().Replace(" ", "").ToLower();
                switch(command)
                {
                    case "start":
                        break;
                    case "reset":
                        jsonData = JsonSerializer.Deserialize<Config>(System.IO.File.ReadAllText(jsonDataPath));
                        Console.WriteLine("INFO: Config data was updated.");
                        break;
                    case "stop":
                        return;
                    default:
                        Console.WriteLine("INFO: Uknown  command.");
                        break;
                }
            }

            var watcher = new FileSystemWatcher(jsonData.folder_a_path);
            watcher.NotifyFilter = NotifyFilters.Attributes
                                 | NotifyFilters.CreationTime
                                 | NotifyFilters.DirectoryName
                                 | NotifyFilters.FileName
                                 | NotifyFilters.LastWrite;
            

            logTimer = new System.Timers.Timer();
            logTimer.Interval = 10000;
            logTimer.AutoReset = true;
            logTimer.Enabled = true;

            watcher.Created += OnCreated;
            watcher.Error += OnError;
            watcher.Filter = "";
            watcher.EnableRaisingEvents = true;

            logTimer.Elapsed += OnTimedEvent;

            
            for (command = ""; command != "reset" && command != "stop"; )
            {
                command = Console.ReadLine().Replace(" ", "").ToLower();
                switch (command)
                {
                    case "reset":
                        jsonData = JsonSerializer.Deserialize<Config>(System.IO.File.ReadAllText(jsonDataPath));
                        Console.WriteLine("INFO: Config data was updated.");
                        command = "";
                        break;
                    case "stop":
                        return;
                    default:
                        Console.WriteLine("INFO: Uknown  command.");
                        break;
                }
            }
        }
        private static void OnCreated(object sender, FileSystemEventArgs e)
        {
            string value = $"Created: {e.FullPath}";
            string cityPattern = @"'[a-z]+,\s[a-z]+\s[0-9]+,\s[0-9]+'",
                   pattern = @"[a-z]+,[a-z]+,'[a-z]+,\s[a-z]+\s\d+,\s\d+',\d+[.]\d+,[0-9]{4}-[0-9]{2}-[0-9]{2},[0-9]+,[a-z]+";

            /// Stage 1 - Reading file names
            Task DirectoryReading = Task.Factory.StartNew(() =>
            {
                var files_name = Directory.GetFiles(jsonData.folder_a_path);
                foreach (var file in files_name)
                {
                    FileInfo fileinf = new FileInfo(file);
                    if (fileinf.Extension == ".txt" || fileinf.Extension == ".csv")
                    {
                        ConcurrencyProperties.filepathes.Enqueue(file);
                    };

                }
            });



            //// Stage 2 - Validating
            var FileValidatingList = new List<Task>();
            for (int i = 0; i < ConcurrencyProperties.tasksNum; i++)
            {
                Task t = Task.Factory.StartNew(() =>
                {
                    Thread.Sleep(30);
                    while (ConcurrencyProperties.filepathes.TryDequeue(out var file))
                    {
                        StreamReader reader = new StreamReader(file);
                        string line;

                        FileInfo fileinf = new FileInfo(file);
                        bool invalidLine = false;
                        if (fileinf.Extension == ".csv") reader.ReadLine();
                        while ((line = reader.ReadLine()) != null)
                        {
                            if (Regex.IsMatch(line, pattern, RegexOptions.IgnoreCase))
                            {
                                ConcurrencyProperties.validLines.Add(line);
                                ConcurrencyProperties.procLineCounter++;
                            }
                            else
                            {
                                ConcurrencyProperties.ErrorCounter++;
                                invalidLine = true;
                            }
                        }
                        if (invalidLine) ConcurrencyProperties.invalidFilesPath.Add(file);
                        ConcurrencyProperties.procFileCounter++;
                        reader.Close(); File.Delete(file);
                    }
                });
                FileValidatingList.Add(t);
            }
            Task.WaitAll(FileValidatingList.ToArray());


            ///Stage 3 - Transofrmation
            var database = from Line in ConcurrencyProperties.validLines
                           let toSplit = Regex.Match(Line, cityPattern, RegexOptions.IgnoreCase).Value
                           let SplitLine = Line.Replace(toSplit + ',', "").Split(',')
                           let city = toSplit.Replace("\'", "").Split(',')[0]
                           select new
                           {
                               city,
                               service_name = SplitLine[5],
                               payer = SplitLine[0] + " " + SplitLine[1],
                               payment = decimal.Parse(SplitLine[2].Replace(".", ",")),
                               date = SplitLine[3],
                               account_num = long.Parse(SplitLine[4])
                           };

            var query =
                from values in database
                group values by values.city into g
                select new TransactionData
                {
                    city = g.Key,
                    services = (from srvc in database
                                where srvc.city == g.Key
                                group srvc by srvc.service_name into h
                                select new Services
                                {
                                    name = h.Key,
                                    payers = (from pymnt in database
                                              where pymnt.service_name == h.First().service_name && pymnt.city == h.First().city
                                              select new Payers { name = pymnt.payer, payment = pymnt.payment, date = pymnt.date, account_number = pymnt.account_num }
                                    ),
                                    total = h.Sum(x => x.payment)
                                }
                               ),
                    total = g.Sum(x => x.payment)
                };

            var options = new JsonSerializerOptions { WriteIndented = true };
            string jsonString = JsonSerializer.Serialize(query, options);
            try
            {
                // Determine whether the directory exists.
                string dirPath = jsonData.folder_b_path + "\\" + DateTime.Now.ToString("dd-MM-yyyy");
                if (!Directory.Exists(dirPath))
                {
                    // Try to create the directory.
                    DirectoryInfo di = Directory.CreateDirectory(dirPath);
                    Console.WriteLine("INFO: The directory was created successfully at {0}.", Directory.GetCreationTime(dirPath));
                }
                FileStream json = File.Create(dirPath + "\\output" + ConcurrencyProperties.fileNumber + ".json");
                StreamWriter jsonWriter = new StreamWriter(json);
                jsonWriter.Write(jsonString); jsonWriter.Flush();
                Console.WriteLine("INFO: JSON file was created successfully at {0}.", Directory.GetCreationTime(dirPath));

            }
            catch (Exception ex)
            {
                Console.WriteLine("INFO: The process failed: {0}", ex.ToString());
            }
            finally { }
            Console.WriteLine(value);
        }
        private static void OnTimedEvent(Object source, System.Timers.ElapsedEventArgs e)     ///Stage 4 - Save
        {
            var fileName = jsonData.folder_b_path + "\\" + DateTime.Now.ToString("dd-MM-yyyy") + "\\file-" + DateTime.Now.ToString("dd-MM-yyyy") + ".log";
            FileStream log = File.Create(fileName);

            StreamWriter logWriter = new StreamWriter(log);
            logWriter.Write("LOG INFO " + DateTime.Now + $"\nparsed_files: {ConcurrencyProperties.procFileCounter}\nparsed_lines: {ConcurrencyProperties.procLineCounter}\nfound_errors: {ConcurrencyProperties.ErrorCounter}\ninvalid_files: [ ");
            foreach (var path in ConcurrencyProperties.invalidFilesPath)
            {
                logWriter.Write(path + ", ");
            }
            logWriter.Write(']');
            logWriter.Flush();
            log.Close();

            ConcurrencyProperties.ErrorCounter = 0;
            ConcurrencyProperties.procLineCounter = 0;
            ConcurrencyProperties.procFileCounter = 0;
            ConcurrencyProperties.fileNumber = 1;
            ConcurrencyProperties.invalidFilesPath = new ConcurrentBag<string>();
        }
        private static void OnDeleted(object sender, FileSystemEventArgs e) =>
            Console.WriteLine($"Deleted: {e.FullPath}");

        private static void OnError(object sender, ErrorEventArgs e) =>
            PrintException(e.GetException());

        private static void PrintException(Exception ex)
        {
            if (ex != null)
            {
                Console.WriteLine($"INFO: {ex.Message}");
                Console.WriteLine("Stacktrace:");
                Console.WriteLine(ex.StackTrace);
                Console.WriteLine();
                PrintException(ex.InnerException);
            }
        }
    }
}
