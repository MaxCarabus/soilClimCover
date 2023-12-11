# начато 2023-12-04
# скрипт для обработки данных почвенного климата
# предварительно импортированных в базы через soil_climate_2base.R
# этап 3 - рассчёт среднемесячных температур и оценка качества данных


library(DBI)
library(lubridate)

# список метеостанций - формируем по набору файлов из папоок с первичкой

setwd('/media/carabus/CARABUS20162/projects/EW_SDM/predictors/meteo/')

# почвенные термометры вытяжные 
tempPStations =  substr(list.files('data/Soil_Temperature/raw/'),4,8)
length(tempPStations) # всего 264 станций, в базе на одну меньше

# почвенные термометры Савинова
tempSStations = substr(list.files('data/Soil_Temperature_Savinov/raw/'),4,8)

# мощность снега на метеостанции 
snowDStations = substr(list.files('data/Snow_Cover/raw/'),4,8)
length(snowDStations)

# маршрутные съемки снежного покрова
snowRStations = substr(list.files('data/Snow_Routes/raw/'),1,5)

rawStations = unique(c(tempPStations,tempSStations,snowDStations,snowRStations))
# всего 906 метеостанций

stations = read.csv('data/stations_vniigmi-mcd.csv')
head(stations)
nrow(stations) # список из 1127 станций

stations[1,1]

setdiff(rawStations, stations$code) # добавлены три метеостанции
# которых не было в общем списке:
# 27331 Ярославль
# 28602 Чистополь
# 28612 Муслюмово (так же указанная с кодом 28612)


# почвенная температура
# вытяжные термометры - 264 метеостанции
# коленчатые термометры - 315
length(unique(c(tempPStations,tempSStations)))
# всего 370 метеостанций

# посчет среднемесячных температур
connection <- dbConnect(RSQLite::SQLite(), "databases/soil_climate.sqlite")

crtable = 'CREATE TABLE IF NOT EXISTS soil_temperature_month (ids integer PRIMARY KEY AUTOINCREMENT,
            station_code integer, year smallint, measure_type varchar, 
            month smallint, depth smallint, t decimal(4,2), quality smallint, days smallint);'
dbExecute(connection, crtable)

mType = 's'
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
            ds = sample(1:30, 20, replace = F)
            ds = sort(ds)
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
          insert = paste0('INSERT INTO soil_temperature_month (station_code,year,',
                          'measure_type,month,depth,t,quality,days) VALUES (',stCode,',',
                          y,',\'',mType,'\',',m,',',dph,',',meanT,',',q,',',daysr,');')
          dbExecute(connection, insert)
        }
      }
    }
  }
  dbCommit(connection) # выполняем SQL тразакцию
}
dbRollback(connection)