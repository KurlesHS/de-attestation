
# Курс 'Инженер данных'. Итоговая аттестация

## Проект "Служба такси"

### Требования:

Дан csv файл с данными по поездок такси в Нью-Йорке, необходимо используя эти данные рассчитать процент поездок по количеству человек в машине (без пассажиров, 1, 2,3,4 и более пассажиров). По итогу должна получиться таблица (parquet) с колонками date, percentage_zero, percentage_1p, percentage_2p, percentage_3p, percentage_4p_plus.

Так же дополнительно необходимо провести аналитику и построить график на тему “как пройденное расстояние и количество пассажиров влияет на чаевые”

### План реализации

1. Установить intellij idea
2. Установить Scala
3. Установить локально Spark
4. Установить VS Code + Anaconda (для построения графика)
5. Реализовать парсинг данных и выгрузку результата согласно требований
6. Провести аналитику и построить график на тему “как пройденное расстояние и количество пассажиров влияет на чаевые”

### Используемые технологии

Для первой части задания было решено использовать язык программирования Scala и фреймворк для обработки неструктурированных и слабоструктурированных данных Spark. Причина выбора именно Spark, а не классического SQL, сугубо учебная, т.к. с SQL я уже знаком на достаточно хорошем уровне.

Для второй части задания используется:

1. Scala + Spark для подготовки данных для визуализации
2. python + matplotlib непосредственно для визуализации
3. python + sklearn для подбора параметров линейной функции

### Архитектура

На вход программе подаётся csv файл '**yellow_tripdata_2020-01.csv**', лежащий уровнем ниже репозитория, на выходе получаем таблицу, сохранённую в parquet-формате, так же лежащую на уровень ниже репозитория в директории '**result.parquet**'

Схема таблицы-результата:

| Имя поля | Тип поля |
|:---------:|:--------:|
| date | Date |
| percentage_zero | Double |
| cheapest_zero | Double |
| dearest_zero | Double |
| percentage_1p | Double |
| cheapest_1p | Double |
| dearest_1p | Double |
| percentage_2p | Double |
| dearest_2p | Double |
| percentage_3p | Double |
| percentage_3p | Double |
| cheapest_3p | Double |
| dearest_3p | Double |
| percentage_4plus |Double |
| cheapest_4plus | Double |
| dearest_4plus | Double |


Во время тестовых прогонов было выявлено, что при работе с dataframe'ом, основанном на csv-файле, spark очень много раз читает этот файл с самого начала, т.е. хоть какая-то индексация для этого формата отсутствует. Для ускорения обработки данных перед тем, как начать обработку, исходный csv-файл конвертируется в формат parquet, что дало значительный прирост производительности.

Для реализации второй части задания в исходный датасет было добавлено дополнительное поле, содержащее округлённое до следующего целого значение пройденного расстояния, затем весь датасет был сгруппирован по этому полю, и для каждого округлённого расстояния выведено среднее значение чаевых, после чего полученный датасет был сохранён в формате csv для последующей визуализации с помощью python. Полученный результат визуализации предоставлен на картинке ниже:

![](https://github.com/KurlesHS/de-attestation/blob/master/images/dist_and_tip.png?raw=true)

Выводы:

1. Полученная витрина содержит 51 запись, т.е. в исходном датасете хранится информация о поездках за 51 день
2. Глядя на график отношения среднего значения чаевых и пройденного расстояния можно заметить, что с увеличением расстояния количество чаевых тоже увеличивается, причем практически линейно. Что логично, так как в США принято оставлять чаевые в размере определённого процента от суммы чека, а чем больше пройденное расстояние, тем выше чек.
3. Так как визуально график получается практически линейным, было решено подобрать параметры линейной функции, максимально подходящий под наши данные. Для этого использовалась библиотека sklearn и ее класс LinearRegression. В результате получилась функция **_y = 0.3781906012902321x + 0.3031735051330209_**, на графике выше она показана оранжевой линией.