# начато 2023-12-04
# скрипт для обработки данных почвенного климата
# предварительно импортированных в базы через soil_climate_2base.R
# этап 3 - рассчёт среднемесячных температур и оценка качества данных

library(DBI)
library(lubridate)

# список метеостанций - формируем по набору файлов из папоок с первичкой

setwd('/media/carabus/Enterprise/EW_SDM/predictors/meteo/')

# почвенные термометры вытяжные 
tempPStations =  substr(list.files('raw/Tpg/'),4,8)
length(tempPStations) # всего 264 станций, в базе на одну меньше

# почвенные термометры Савинова
tempSStations = substr(list.files('raw/Tpgks/'),4,8)

# мощность снега на метеостанции 
snowDStations = substr(list.files('raw/Snow/'),4,8)
length(snowDStations)

# маршрутные съемки снежного покрова
snowRStations = substr(list.files('raw/SnMar/'),1,5)

rawStations = unique(c(tempPStations,tempSStations,snowDStations,snowRStations))
# всего 906 метеостанций

stations = read.csv('weather_stations/stations_vniigmi-mcd.csv')
head(stations)
nrow(stations) # список из 1127 станций

setdiff(rawStations, stations$code) # добавлены три метеостанции
# которых не было в общем списке:
# 27331 Ярославль
# 28602 Чистополь
# 28612 Муслюмово (так же указанная с кодом 28611)

# почвенная температура
# вытяжные термометры - 264 метеостанции
# коленчатые термометры - 315
length(unique(c(tempPStations,tempSStations)))
# всего 370 метеостанций

# посчет среднемесячных температур
connection <- dbConnect(RSQLite::SQLite(), "databases/soil_climate_temperature.sqlite")

crtable = 'CREATE TABLE IF NOT EXISTS soil_temperature_monthly (ids integer PRIMARY KEY AUTOINCREMENT,
            station_code integer, year smallint, measure_type varchar, 
            month smallint, depth smallint, t decimal(4,2), quality smallint, days smallint);'
dbExecute(connection, crtable)

mType = 'p'
# рассчитываем среднемесячные температуры
# 1. Список метеостанций 
select = paste0('SELECT DISTINCT station_id FROM soil_temperature WHERE',
                ' measure_type = \'',mType,'\' ORDER BY station_id;')
                
stationIDs = dbGetQuery(connection, select)$station_id

stCount = length(stationIDs) # 263 - одной не хватает - добавлено
setdiff(tempPStations, stationIDs) # 22471 не импортирована в базу - удалена не преднамеренно
setdiff(tempSStations, stationIDs)

cat(paste0('Всего найдено метеостанций: ', stCount))

# расчёт средних значений и заполнение таблицы
for (i in 1 : stCount) { # итерация по метеостанциям  
  # i = 1
  dbBegin(connection) # открываем SQL транзакцию
  stCode = stationIDs[i]
  cat(paste0('вычисляем средние значения для станции: ',stCode,' - ',i,' из ',stCount,'\n'))
  
  # 2. Диапазон данных по метеостанции и позиции по глубинам
  select = paste0('SELECT date, depth, t, quality FROM soil_temperature WHERE measure_type = \'',
                  mType,'\' AND station_id = ',stCode)
  soilData = dbGetQuery(connection, select)
  soilData$year  = as.integer(substr(soilData$date,1, 4))
  soilData$month = as.integer(substr(soilData$date, 6,7))
  soilData$day   = as.integer(substr(soilData$date,9,10))
   
  firstDate = min(soilData$date)
  lateDate  = max(soilData$date)
  
  # диапазон лет
  firstYear = as.integer(substr(firstDate,1,4))
  lateYear  = as.integer(substr(lateDate,1,4))
  
  # глубины, по которым происходят измерения
  depthes = unique(soilData$depth)
  
  # расчитываем средние значения, с оценкой качества
  for (d in 1 : length(depthes)) { # итерация по глубинам
    # d = 1
    dph = depthes[d] # очередная глубина
    cat(paste0('\tобрабатываем глубину ',dph,' см на метеостанции ',stCode,'\n'))
    for (y in firstYear : lateYear) { # итерация по годам
      # y = 1991
      for (m in 1:12) { # итерация по месяцам
        # m = 6 
        monthRow = soilData[soilData$depth == dph & soilData$year == y & soilData$month == m,]
        daysm = days_in_month(monthRow$date[1]) # сколько должно быть в месяце
        daysr = length(monthRow$date) # сколько на самом деле дней
        
        # если ни одного дня наблюдений нет - пропускаем
        if (daysr == 0) {
          cat(paste0('\t\tгод: ',y,', месяц: ',m,'\tданных нет\n'))
        } else {
          # далее оцениваем качество
          # все дни есть - всё нормально
          if (daysr == 0) {
            q = 0   # данных нет вообще
          } else if (daysm == daysr) {
            q = 1   # всё есть без пропусков
          } else if (daysm - daysr > 5) {
            q = 5   # пропущено более 5 дней - некондиция 
          } else if (daysm - daysr < 4) {
            q = 2   # пропущено не более 3 дней - можно использовать
          } else { # не более 5ти пропусков и не более 3х подряд
            q = 3   # пропущено не более 5 дней и не более 3х дней подряд - можно использовать
            ds = monthRow$day
            # ds = sample(1:30, 20, replace = F)
            # ds = sort(ds)
            # daysr = 20
            for (d in 1 : (daysr-1)) {
              cat(paste0(d,' \t'))
              df = ds[d+1] - ds[d]
              if (df > 4) q = 4 # пропущено не более 5 дней, более 3х дней подряд - некондиция
            }
          }
          # собственно, среднее значение
          meanT = round(mean(monthRow$t),2)
          cat(paste0(stCode,' ',dph,' год: ',y,', месяц: ',m,'\tсредняя t: ',meanT,'\tкачество: ',q,' число дней: ',daysr,'\n'))
          
          # заносим в таблицу 
          insert = paste0('INSERT INTO soil_temperature_monthly (station_code,year,',
                          'measure_type,month,depth,t,quality,days) VALUES (',stCode,',',
                          y,',\'',mType,'\',',m,',',dph,',',meanT,',',q,',',daysr,');')
          dbExecute(connection, insert)
        }
      }
    }
  }
  dbCommit(connection) # выполняем SQL тразакцию
}

# удаляем записи по метеостанции, где расчитаны не все среднемесячные значения
# delete = 'DELETE FROM soil_temperature_month WHERE ids > 961180;'
# dbExecute(connection, delete)
# dbRollback(connection) # если чего-то не доделали

# мощность снежного покрова
connection <- dbConnect(RSQLite::SQLite(), "databases/soil_climate_snow.sqlite")

delTable = 'DROP TABLE snow_cover_monthly;'
dbExecute(connection, delTable)

create = 'CREATE TABLE IF NOT EXISTS snow_cover_monthly (ids integer PRIMARY KEY AUTOINCREMENT,
            station_code integer, year smallint, month smallint, depth decimal(4,1), 
            quality smallint, days smallint);'
dbExecute(connection, create)

select = paste0('SELECT DISTINCT station_id FROM snow_cover_daily ORDER BY station_id;')
stationIDs = dbGetQuery(connection, select)$station_id
stCount = length(stationIDs) # проверяем, всё ли импортировано

# setdiff(tempPStations, stationIDs) # 22471 не импортирована в базу - удалена не преднамеренно

cat(paste0('Всего найдено метеостанций: ', stCount))

# расчёт средних значений и заполнение таблицы
for (i in 1 : stCount) { # итерация по метеостанциям  

  dbBegin(connection) # открываем SQL транзакцию
  stCode = stationIDs[i]
  cat(paste0('вычисляем средние значения для станции: ',stCode,' - ',i,' из ',stCount,'\n'))
  
  # 2. Диапазон данных по метеостанции и позиции по глубинам
  select = paste0('SELECT measured, depth, quality FROM snow_cover_daily ', 
                  'WHERE station_id = ',stCode,' AND depth <> 9999;')
  snowData = dbGetQuery(connection, select)
  
  # dbExecute(connection, 'DELETE FROM snow_cover_daily WHERE ids = 18760812;')
  # dbCommit(connection)
  
  snowData$year  = as.integer(substr(snowData$measured,1, 4))
  snowData$month = as.integer(substr(snowData$measured, 6,7))
  snowData$day   = as.integer(substr(snowData$measured,9,10))
  
  firstDate = min(snowData$measured) # 1958-01-01
  lateDate  = max(snowData$measured) # 2021-12-31
  
  # диапазон лет
  firstYear = as.integer(substr(firstDate,1,4))
  lateYear  = as.integer(substr(lateDate,1,4))
 
  # расчитываем средние значения, с оценкой качества
    for (y in firstYear : lateYear) { # итерация по годам
      # y = 1958
      for (m in 1:12) { # итерация по месяцам
        # m = 1
        monthRow = snowData[snowData$year == y & snowData$month == m,]
        daysm = days_in_month(monthRow$measured[1]) # сколько должно быть в месяце
        daysr = length(monthRow$measured) # сколько на самом деле дней
        
        # если ни одного дня наблюдений нет - пропускаем
        if (daysr == 0) {
          q = 0
          cat(paste0('\t\tгод: ',y,', месяц: ',m,'\tданных нет\n'))
        } else {
          # далее оцениваем качество
          # все дни есть - всё нормально
          if (daysr == 0) {
            q = 0   # данных нет вообще
          } else if (daysm == daysr) {
            q = 1   # всё есть без пропусков
          } else if (daysm - daysr > 5) {
            q = 5   # пропущено более 5 дней - некондиция 
          } else if (daysm - daysr < 4) {
            q = 2   # пропущено не более 3 дней - можно использовать
          } else { # не более 5ти пропусков и не более 3х подряд
            q = 3   # пропущено не более 5 дней и не более 3х дней подряд - можно использовать
            ds = monthRow$day
            # ds = sample(1:30, 20, replace = F)
            # ds = sort(ds)
            # daysr = 20
            for (d in 1 : (daysr-1)) {
              cat(paste0(d,' \t'))
              df = ds[d+1] - ds[d]
              if (df > 4) q = 4 # пропущено не более 5 дней, более 3х дней подряд - некондиция
            }
          }
          # собственно, среднее значение
          meanDepth = round(mean(monthRow$depth),1)
          cat(paste0(stCode,' год: ',y,', месяц: ',m,'\tсредняя глубина: ',meanDepth,'\tкачество: ',q,' число дней: ',daysr,'\n'))
          
          # заносим в таблицу 
          insert = paste0('INSERT INTO snow_cover_monthly (station_code,year,',
                          'month,depth,quality,days) VALUES (',stCode,',',
                          y,',',m,',',meanDepth,',',q,',',daysr,');')
          dbExecute(connection, insert)
        }
      }
    }
  dbCommit(connection) # выполняем SQL тразакцию
}
dbRollback(connection)