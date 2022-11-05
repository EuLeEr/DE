//7. Написать программу, которая выводит таблицу значений функции y=-2 * x^2 - 5 * x - 8 в диапазоне от –4 до +4, с шагом 0,5

#include <iostream>

using namespace std;

int tableSolution()
{
    double i = -4;
    while (i <= 4)
    {
        cout << i << " " << -2 * i * i - 5 * i - 8 << endl;
        i += 0.5;
    }
    
    return 0;
}
int main(int argc, char const *argv[])
{
    /* code */
   tableSolution();
}
