
#include <iostream>
#include <string>
#include <locale.h>

using namespace std;

int inputTwoNumberAndCompareThem()
{   int a;
    int b;
    cout << "Введите два числа: ";
    cin >> a >> b;
    if (a > b)
    {
        cout << "Первое число больше  чем второе";
    }
    else if (a < b)
    {
        cout << "Первое число меньше  чем второе";
    }
    else
    {
        cout << "Первое число равно  второму";
    }
    return 0;
}

int main (){
    inputTwoNumberAndCompareThem();
    }