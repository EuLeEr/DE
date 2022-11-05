#include <iostream>

using namespace std;

int tableOfSquaresOfTheFirstTenPositiveNumbers()
{
    int i = 1;
    for  (;i <= 10;)
    {
        cout << i << " " << i * i << endl;
        i++;
    }
    return 0;
}
int main(int argc, char const *argv[])
{
    /* code */
   tableOfSquaresOfTheFirstTenPositiveNumbers();
}
