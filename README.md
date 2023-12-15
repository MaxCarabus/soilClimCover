## Слои почвенного климата для Европейской части России

### список файлов
скрипты:
1. ***soil_climate_2base.R*** - импорт данных из файлов с первичными данными.<BR>
   используются пакеты: DBI, stringr<BR>
   Предполагается, что первичные данные уже получены из репозитория ВНИИГМИ-МЦД и распакованы в директорию **predictors/meoteo/raw** 
   по соответсвующим папкам:<br>
   **Tpg/** - данные о температуре почвы на глубинах до 320 см<br>
   **Tpgks/** - данные о температуре почвы по Коленчатым термометрам Савинова<br>
   **Snow/** - характеристики снежного покрова на метеостанциях<br>
   **SnMar/** - маршрутне снегомерные съемки<br>
   **Tttr/** - суточная температура воздуха и количество осадков (*процедура импорта и соответствующая база в разработке*)<br>
   Обработка нескольких сотен файлов занимает не один час, базы данных с импортированными файлами будут находится в директории **predictors/meteo/databases**<br>
   ***soil_climate_temperature.sqlite*** - температура почвы на разных глубинах (массивы данных **Tpg** и **Tpgks**)<br>
   ***soil_climate_snow.sqlite*** - снежный покров (массивы данных **Snow** и **SnMar**)

2. ***soil_climate_overview.R*** - сводка по метеостациям с данными почвенного климата, импортированными в базу
   в том числе визуализация данных<br>
   используемые пакеты: sf, stringr, ggplot2<br>
   Схема расположения метеостанций на территории интереса и окресностях<br>
   ![СХЕМА](https://github.com/MaxCarabus/soilClimCover/blob/main/weather_stations_aoi.png)<br>
   ***aoi_albers.kml*** - векторный слой формата KML, территория интереса в равновеликой конической проекции альберса<br>
   ***aoi_dd.kml*** - векторный слой формата KML, территория интереса в десятичных градусах<br>
   ***Russia_European_albers.zip*** - слой формат shape, территория интереса в равновеликой конической проекции альберса<br>
   ***weather_stations.zip*** - слой формата shape и таблицы со списками метеостанций<br>

3. ***soil_climate_processing.R*** - рассчёт среднемесячных температур и оценка качества данных
   используемые пакеты: DBI, lubridate

4. ***soil_climate_layers.R*** - генерация растровых слоёв почвенного климата<br><br><br>


   используемые пакеты: DBI, sf, geodata, ggplot2, raster, fields


***soil_climate_layers.zip*** - все скрипты и входные данные (кроме архива метеонаблюдений), а также структура директорий<br>
Исследование поддержано грантом Российского Научного Фонда № [23-24-00112](https://rscf.ru/en/project/23-24-00112/)
