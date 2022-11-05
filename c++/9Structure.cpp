//9. Создайте структуру с именем student, содержащую поля: фамилия и инициалы, номер группы, успеваемость (массив из пяти элементов).
// Создать массив из десяти элементов такого типа, упорядочить записи по возрастанию среднего балла. 
// Добавить возможность вывода фамилий и номеров групп студентов, имеющих оценки, равные только 4 или 5.
#include <iostream>
#include <string>
using namespace std;
int CreateStructureStudent(){
    struct student
    {
        string name;
        int group;
        int marks[6];
    };
    student students[10];
    students[0].name = "Иванов И.И.";
    students[0].group = 1;
    students[0].marks[0] = 4;
    students[0].marks[1] = 5;
    students[0].marks[2] = 4;
    students[0].marks[3] = 5;
    students[0].marks[4] = 4;
    students[1].name = "Петров П.П.";
    students[1].group = 2;
    students[1].marks[0] = 5;
    students[1].marks[1] = 5;
    students[1].marks[2] = 5;
    students[1].marks[3] = 5;
    students[1].marks[4] = 5;
    students[2].name = "Сидоров С.С.";
    students[2].group = 3;
    students[2].marks[0] = 4;
    students[2].marks[1] = 4;
    students[2].marks[2] = 4;
    students[2].marks[3] = 4;
    students[2].marks[4] = 4;
    students[3].name = "Алексеев А.А.";
    students[3].group = 4;
    students[3].marks[0] = 5;
    students[3].marks[1] = 5;
    students[3].marks[2] = 5;
    students[3].marks[3] = 5;
    students[3].marks[4] = 5;
    students[4].name = "Андреев А.А.";
    students[4].group = 5;
    students[4].marks[0] = 4;
    students[4].marks[1] = 4;
    students[4].marks[2] = 4;
    students[4].marks[3] = 4;
    students[4].marks[4] = 4;
    students[5].name = "Александров А.А.";
    students[5].group = 6;
    students[5].marks[0] = 5;
    students[5].marks[1] = 5;
    students[5].marks[2] = 5;
    students[5].marks[3] = 5;
    students[5].marks[4] = 5;
    students[6].name = "Анатольев А.А.";
    students[6].group = 7;
    students[6].marks[0] = 4;
    students[6].marks[1] = 4;
    students[6].marks[2] = 4;
    students[6].marks[3] = 4;
    students[6].marks[4] = 4;   
    students[7].name = "Антонов А.А.";
    students[7].group = 8;
    students[7].marks[0] = 5;
    students[7].marks[1] = 5;
    students[7].marks[2] = 5;
    students[7].marks[3] = 5;
    students[7].marks[4] = 5;
    students[8].name = "Артемов А.А.";
    students[8].group = 9;  
    students[8].marks[0] = 4;
    students[8].marks[1] = 4;
    students[8].marks[2] = 4;
    students[8].marks[3] = 4;
    students[8].marks[4] = 4;
    students[9].name = "Арсеньев А.А.";
    students[9].group = 10;
    students[9].marks[0] = 3;
    students[9].marks[1] = 3;
    students[9].marks[2] = 3;
    students[9].marks[3] = 3;
    students[9].marks[4] = 3;
    for (int i = 0; i < 10; i++)
    {
        int sum = 0;
        for (int j = 0; j < 5; j++)
        {
            sum += students[i].marks[j];
        }
        students[i].marks[5] = sum / 5;
    }

    for (int i = 0; i < 10; i++)
    {
        for (int j = 0; j < 10; j++)
        {
            if (students[i].marks[5] < students[j].marks[5])
            {
                student temp = students[i];
                students[i] = students[j];
                students[j] = temp;
            }
        }
    }
    for (int i = 0; i < 10; i++)
    {
        for (int j = 0; j < 5; j++)
        {
            if (students[i].marks[j] == 4 || students[i].marks[j] == 5)
            {
                cout << students[i].name << " " << students[i].group << endl;
                break;
            }
        }
    }
    return 0;
}

int main(int argc, char const *argv[])
{
    /* code */
    CreateStructureStudent();
}