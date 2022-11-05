//10. Создайте структуру с именем train, содержащую поля: название пункта назначения, номер поезда, время отправления. 
//Ввести данные в массив из пяти элементов типа train, упорядочить элементы по номерам поездов. 
//Добавить возможность вывода информации о поезде, номер которого введен пользователем. 
// Добавить возможность сортировки массив по пункту назначения, причем поезда с одинаковыми пунктами назначения должны быть 
// упорядочены по времени отправления.
#include <iostream>
#include <string>


using namespace std;
int CreateStructureTrain()
{
    struct train
    {
        string destination;
        int trainNumber;
        string departureTime;
    };
    train trains[5];
    trains[0].destination = "Москва";
    trains[0].trainNumber = 1;
    trains[0].departureTime = "10:00";
    trains[1].destination = "Санкт-Петербург";
    trains[1].trainNumber = 2;
    trains[1].departureTime = "11:00";
    trains[2].destination = "Владивосток";
    trains[2].trainNumber = 3;
    trains[2].departureTime = "12:00";
    trains[3].destination = "Калининград";
    trains[3].trainNumber = 4;
    trains[3].departureTime = "13:00";
    trains[4].destination = "Калининград";
    trains[4].trainNumber = 5;
    trains[4].departureTime = "14:00";
    //ввести номер поезда
    int trainNumber;
    cout << "Введите номер поезда: ";
    cin >> trainNumber;
    //вывести информацию о поезде
    for (int i = 0; i < 5; i++)
    {
        if (trainNumber == trains[i].trainNumber)
        {
            cout << "Пункт назначения: " << trains[i].destination << endl;
            cout << "Номер поезда: " << trains[i].trainNumber << endl;
            cout << "Время отправления: " << trains[i].departureTime << endl;
        }
    }

    for (int i = 0; i < 5; i++) {
        for (int j = 0; j < 5; j++)
            {
                if (trains[i].trainNumber < trains[j].trainNumber)
                {
                    train temp = trains[i];
                    trains[i] = trains[j];
                    trains[j] = temp;
                }
            }
    }
  
    for (int i = 0; i < 5; i++) {
        for (int j = 0; j < 5; j++)
            {
                if (trains[i].trainNumber = trains[j].trainNumber) {
                    if (trains[i].departureTime < trains[j].departureTime)
                    {
                        train temp = trains[i];
                        trains[i] = trains[j];
                        trains[j] = temp;
                    }
                }
            }
    }
    // for (int i = 0; i < 5; i++)
    // {
    //     cout << trains[i].destination << " " << trains[i].trainNumber << " " << trains[i].departureTime << endl;
    // }
    // return 0;
}
int main(int argc, char const *argv[])
{
    /* code */
    CreateStructureTrain();
}
