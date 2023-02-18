using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ConsoleApp1
{
    internal class Program
    {
        static void Main(string[] args)
        {
            string FilesPath = "D:\\Student\\Radency\\Task1\\ConsoleApp1\\ConsoleApp1\\Data";
            var files_name = Directory.GetFiles(FilesPath);
            foreach (var file in files_name)
            {
                FileInfo fileinf = new FileInfo(file);
                if (fileinf.Extension == ".txt" || fileinf.Extension == ".csv") Console.WriteLine(file);

            }
            Console.ReadLine();
        }
    }
}
