#include <iostream>

using namespace std;

int main()
{
    int number;
    cout << "Введите число: ";
    cin >> number;
    if (number % 2 == 0 && number != 0)
    {
        cout << "Число четное";
    }
    else if (number % 2 != 0)
    {
        cout << "Число нечетное";
    }
    else
    {
        cout << "Число равно нулю";
    }
    
    return 0;
}   