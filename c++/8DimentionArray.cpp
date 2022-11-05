//Необходимо создать двумерный массив 5 х 5. Далее написать функцию, которая заполнит его случайными числами от 30 до 60. 
//Создать еще две функции, которые находят максимальный и минимальный элементы этого двумерного массива.
#include <iostream>
#include <cmath>
#include <ctime>
#include <chrono>
#include <thread>
using namespace std;
using std::this_thread::sleep_for;
using std::chrono::milliseconds;

int GetRandomNumber(int min, int max)
{
  // Установить генератор случайных чисел
  srand(time(NULL));
  sleep_for(std::chrono::milliseconds(100));
  // Получить случайное число - формула
  int num = min + rand() % (max - min + 1);

  return num;
}
void GetMaxFrom2DimentionArray(int ** array)
{
    int max = array[0][0];
    for (int i = 0; i < 5; i++)
    {
        for (int j = 0; j < 5; j++)
        {
            if (max < array[i][j])
            {
                max = array[i][j];
            }
        }
    }
    cout << "Максимальный элемент массива: " << max << endl;
}
void GetMinFrom2DimentionArray(int ** array)
{
    int min = array[0][0];
    for (int i = 0; i < 5; i++)
    {
        for (int j = 0; j < 5; j++)
        {
            if (min > array[i][j])
            {
                min = array[i][j];
            }
        }
    }
    cout << "Минимальный элемент массива: " << min << endl;
}

int createTwoDimentionArray() {
    int n, m;
/*    cout << "Введите количество строк: ";
    cin >> n;
    cout << "Введите количество столбцов: ";
    cin >> m; */
    n = 5;
    m = 5;
    int **a = new int *[n];
    for (int i = 0; i < n; i++) {
        a[i] = new int[m];
    }
    for (int i = 0; i < n; i++) {
        for (int j = 0; j < m; j++) {
            a[i][j] = GetRandomNumber(30,60);
        }
    }
    for (int i = 0; i < n; i++) {
        for (int j = 0; j < m; j++) {
            cout << a[i][j] << " ";
        }
        cout << endl;
    }
    GetMaxFrom2DimentionArray( a);
    GetMinFrom2DimentionArray( a);
 //   GetMin(n, m, a);
    for (int i = 0; i < n; i++) {
        delete[] a[i];
    }
    delete[] a;
    return 0;
}  




int main(int argc, char const *argv[])
{
    /* code */
   createTwoDimentionArray();

}