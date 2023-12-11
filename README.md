## Слои почвенного климата для Европейской части России


### список файлов
скрипты:
1. soil_climate_2base.R - импорт данных из файлов с первичными данными.
   используются пакеты: DBI, stringr

2. soil_climate_overview.R - сводка по метеостациям с данными почвенного климата, импортированными в базу
   в том числе визуализация данных 
   используемые пакеты: sf, stringr, ggplot2

3. soil_climate_processing.R - рассчёт среднемесячных температур и оценка качества данных
   используемые пакеты: DBI, lubridate

4. soil_climate_layers.R - генерация растровых слоёв почвенного климата
   используемые пакеты: DBI, sf, geodata, ggplot2


aoi_albers.kml - территория интереса в равновеликой конической проекции альберса
aoi_dd,kml - территория интереса в десятичных градусах