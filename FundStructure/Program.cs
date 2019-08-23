using System;

namespace FundStructure
{
    class Program
    {
        static void Main(string[] args)
        {
            
            // This is number guess game
            Random myRandom = new Random();
            int s_n = myRandom.Next(0, 1000);

            Console.WriteLine("what is the secret number?");
            string y_o_b = Console.ReadLine();
            int yob = int.Parse(y_o_b);
            int i = 0;
            while (yob != s_n)
            {
                if (i > 8)
                {
                    Console.WriteLine("Never seen an idiot like you! Count value 10! 10 chances wasted!!!");
                    Console.WriteLine("The secret number is " + s_n);
                    break;
                }
                if (yob > s_n)
                {
                    Console.WriteLine("Too big! Try again! Count value is " + (i + 1));
                    y_o_b = Console.ReadLine();
                    yob = int.Parse(y_o_b);
                    i++;
                }

                if (yob < s_n)
                {
                    Console.WriteLine("Too samll! Try again! Count value is " + (i + 1));
                    y_o_b = Console.ReadLine();
                    yob = int.Parse(y_o_b);
                    i++;
                }


                if (yob == s_n)
                {
                    Console.WriteLine(" Correct! You are a genius!");
                }

            }

        }
    }
}
