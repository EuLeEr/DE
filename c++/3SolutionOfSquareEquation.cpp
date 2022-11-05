#include <iostream>
#include <cmath>
using namespace std;

int SolutionOfSquareEquation()
{
    double a;
    double b;
    double c;
    double D;
    double x1;
    double x2;
    cout << "Введите коэффициенты квадратного уравнения: ";
    do  {
        cin >> a >> b >> c;
        if (a == 0) {
            cout << "Коэффициент a не может быть равен нулю. Введите коэффициенты заново: ";
        }
    } 
    while (a == 0);
    D = b * b - 4 * a * c;
    if (D > 0)
    {
        x1 = (-b + sqrt(D)) / (2 * a);
        x2 = (-b - sqrt(D)) / (2 * a);
        cout << "x1 = " << x1 << endl;
        cout << "x2 = " << x2 << endl;
    }
    else if (D == 0)
    {
        x1 = -b / (2 * a);
        cout << "x1 = x2 = " << x1 << endl;
    }
    else
    {
        cout << "Корней нет" << endl;
    }
    return 0;
}

int main()
{
    SolutionOfSquareEquation();
}