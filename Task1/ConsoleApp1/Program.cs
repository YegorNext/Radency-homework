using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Collections.Concurrent;
using System.Threading;


namespace ConsoleApp1
{
    //////////////// Опис конфігураційного файлу .json //////////////// 
    public class Config 
    {
        public string folder_a_path { get; set; }
        public string folder_b_path { get;set; }

    }

    //////////////// Опис полів вихідного файлу даних .json //////////////// 
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


    //////////////// Опис полів для паралельної обробки //////////////// 
    public static class ConcurrencyProperties 
    { 
        public const int tasksNum = 3; // кількість завдань 
        public static int ErrorCounter = 0; // кількість файлів з помилками
        public static int procLineCounter = 0; // кількість оброблених рядків
        public static int procFileCounter = 0; // кількість оброблених файлів
        public static int fileNumber = 1; // номер файлу json для створення
        public static ConcurrentQueue<string> filepathes = new ConcurrentQueue<string>(); // шляхи файлів в директорі, що задана в .json
        public static ConcurrentBag<string> validLines = new ConcurrentBag<string>(); // валідні рядки
        public static ConcurrentBag<string> invalidFilesPath = new ConcurrentBag<string>(); // шляхи невалідних файлів

    }
    internal class Program
    {
        private static Config jsonData = null; // об'єкт конфігураційного файлу
        private static System.Timers.Timer logTimer; // таймер створення log файлу
        static void Main(string[] args)
        {
            //////////////// Десереалізація json cfg файлу  //////////////// 
            string jsonDataPath = "D:\\Student\\Radency\\Task1\\ConsoleApp1\\Task1\\ConsoleApp1\\config.json"; // шлях до конфігураційного файлу
            string command = ""; // команди консолі
            try // намагаємося десереалізувати дані json файлу
            {
                jsonData = JsonSerializer.Deserialize<Config>(System.IO.File.ReadAllText(jsonDataPath));
            }
            catch(Exception e) // перехоплюємо виключення
            {
                Console.WriteLine("INFO/ERROR: Can't read json data file, {0}", e.ToString()); // виводому помилку 
                return; // завершуємо програму
            }


            //////////////// вітання при запуску програми  //////////////// 
            Console.WriteLine("########Welcome to JSON Creator v3.0########\nPlease, choose an action (start & reset & stop)"); 
            while(command != "start" && command != "stop") // обираємо дію
            {
                command = Console.ReadLine().Replace(" ", "").ToLower();
                switch(command)
                {
                    case "start": // продовжуємо виконання коду(запускаємо програму)
                        break;
                    case "reset": // відновлюємо дані файлу json
                        jsonData = JsonSerializer.Deserialize<Config>(System.IO.File.ReadAllText(jsonDataPath));
                        Console.WriteLine("INFO: Config data was updated.");
                        break;
                    case "stop": // припиняємо роботу додатку
                        return;
                    default:
                        Console.WriteLine("INFO: Uknown  command.");
                        break;
                }
            }


            //////////////// ствоюємо watcher директорії  //////////////// 
            var watcher = new FileSystemWatcher(jsonData.folder_a_path); 
            watcher.NotifyFilter = NotifyFilters.Attributes
                                 | NotifyFilters.CreationTime
                                 | NotifyFilters.DirectoryName
                                 | NotifyFilters.FileName
                                 | NotifyFilters.LastWrite;



            //////////////// Налатшовуємо Timer  //////////////// 
           
            logTimer = new System.Timers.Timer();
            logTimer.Interval = 10000; // 10 секунд для тестування 
            logTimer.AutoReset = true;
            logTimer.Enabled = true;


            //////////////// Стоврюємо події  //////////////// 

            watcher.Created += OnCreated;
            watcher.Error += OnError;
            watcher.Filter = "";
            watcher.EnableRaisingEvents = true;

            logTimer.Elapsed += OnTimedEvent;


            ////////////////  Очікуємо команди користувача  ////////////////
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

        ////////////////  Обробник події створення файлу в директорії  ////////////////
        private static void OnCreated(object sender, FileSystemEventArgs e)
        {
            string value = $"Created: {e.FullPath}";
            string cityPattern = @"'[a-z]+,\s[a-z]+\s[0-9]+,\s[0-9]+'",
                   pattern = @"[a-z]+,[a-z]+,'[a-z]+,\s[a-z]+\s\d+,\s\d+',\d+[.]\d+,[0-9]{4}-[0-9]{2}-[0-9]{2},[0-9]+,[a-z]+";

            ////////////////  Stage 1 - reading file names  ////////////////
            Task DirectoryReading = Task.Factory.StartNew(() =>   // запускаємо задачу на виконання 
            {
                var files_name = Directory.GetFiles(jsonData.folder_a_path);
                foreach (var file in files_name)
                {
                    FileInfo fileinf = new FileInfo(file);
                    if (fileinf.Extension == ".txt" || fileinf.Extension == ".csv")
                    {
                        ConcurrencyProperties.filepathes.Enqueue(file); // заповнюємо чергу pipeline
                    };

                }
            });



            ////////////////  Stage 2 - Validating  ////////////////
            var FileValidatingList = new List<Task>(); // стоврюємо лист завдань
            for (int i = 0; i < ConcurrencyProperties.tasksNum; i++)
            {
                Task t = Task.Factory.StartNew(() => // створюємо декілька завдань
                {
                    Thread.Sleep(30);
                    while (ConcurrencyProperties.filepathes.TryDequeue(out var file)) // обробляємо рядки з файлу
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
                        reader.Close(); File.Delete(file); // закриваємо та видаляємо файл після обробки
                    }
                });
                FileValidatingList.Add(t);
            }
            Task.WaitAll(FileValidatingList.ToArray()); // очікуємо завершення усіх задач з листу


            ////////////////  Stage 3 - Transformation data to JSON  ////////////////
            var database = from Line in ConcurrencyProperties.validLines // використовуючи LINQ створюємо базу даних
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

            var query = // виконуємо запит даних
                from values in database
                group values by values.city into g
                select new TransactionData
                {
                    city = g.Key,
                    services = (from srvc in database // виконуємо підзапии
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
            ConcurrencyProperties.validLines = new ConcurrentBag<string>(); // оновляємо bag
            ConcurrencyProperties.fileNumber++; // збільшуємо лічильник


            ////////////////  Сериалізація даних  ////////////////
            var options = new JsonSerializerOptions { WriteIndented = true };
            string jsonString = JsonSerializer.Serialize(query, options);
            try // намагаємося створити директорію
            {
                
                string dirPath = jsonData.folder_b_path + "\\" + DateTime.Now.ToString("dd-MM-yyyy");
                if (!Directory.Exists(dirPath))
                {
                    DirectoryInfo di = Directory.CreateDirectory(dirPath);
                    Console.WriteLine("INFO: The directory was created successfully at {0}.", Directory.GetCreationTime(dirPath));
                }
                FileStream json = File.Create(dirPath + "\\output" + ConcurrencyProperties.fileNumber + ".json");
                StreamWriter jsonWriter = new StreamWriter(json);
                jsonWriter.Write(jsonString); 
                jsonWriter.Flush(); 
                jsonWriter.Close(); 
                Console.WriteLine("INFO: JSON file was created successfully at {0}.", Directory.GetCreationTime(dirPath));

                

            }
            catch (Exception ex)
            {
                Console.WriteLine("INFO: The process failed: {0}", ex.ToString());
            }
            finally { }
            Console.WriteLine(value);
        }

        ////////////////  Обробник події таймеру. Створення meta.log  ////////////////
        private static void OnTimedEvent(Object source, System.Timers.ElapsedEventArgs e)     ///Stage 4 - Save
        {
            var fileName = jsonData.folder_b_path + "\\meta" + ".log";
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

            // Оновлюємо дані //
            ConcurrencyProperties.ErrorCounter = 0;
            ConcurrencyProperties.procLineCounter = 0;
            ConcurrencyProperties.procFileCounter = 0;
            ConcurrencyProperties.fileNumber = 1;
            ConcurrencyProperties.invalidFilesPath = new ConcurrentBag<string>();
        }

        ////////////////  Обробник події видалення оброблених файлів з директорії ////////////////
        private static void OnDeleted(object sender, FileSystemEventArgs e) =>
            Console.WriteLine($"Deleted: {e.FullPath}");

        ////////////////  Обробник події виникнення помилки ////////////////
        private static void OnError(object sender, ErrorEventArgs e) =>
            PrintException(e.GetException());


        ////////////////  Повідомлення про exception ////////////////
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
