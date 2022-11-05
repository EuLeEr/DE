//Задача на цикл do while

//6. Написать программу, которая определяет максимальное число из введённой с клавиатуры последовательности положительных чисел. 
//(длина последовательности неограниченна)
#include <iostream>

using namespace std;

int tableSolution()
{
    int i;
    int max = 0;
    do 
    {
        cin >> i;
         
        if (max < i)
        {
            max = i;
        }
    }
    while (i != 0);
    cout << max;
    return 0;
}
int main(int argc, char const *argv[])
{
    /* code */
   tableSolution();
}
