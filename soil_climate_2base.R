# 2023-12-03
# этап 1
# импорт метеоданных в базу

# Какой массив данных импортируем в базу
Pulling = TRUE # вытяжные термометры 
Savinov = FALSE # коленчатые термометры Савинова

##### -------- Soil Temperature -------
# задаём путь к папке проекта
setwd('/media/carabus/Enterprise/EW_SDM/')
library(DBI)
library(stringr)

# подключение к базе, если файла нет - автоматически создаётся пустой
connection <- dbConnect(RSQLite::SQLite(), "predictors/meteo/databases/soil_climate.sqlite")

# создаём структуру - таблица для импотра
crtable = 'CREATE TABLE IF NOT EXISTS soil_temperature (ids integer PRIMARY KEY AUTOINCREMENT,
          station_id integer, date date, depth smallint, measure_type varchar, 
          t decimal(3,1), quality smallint);'
dbExecute(connection, crtable)

# удаляем недоимпортированный файл, если импорт прервался, далее с него начинаем
# dbBegin(connection) # начало сессии
# del = 'DELETE FROM soil_temperature WHERE station_id = 22471'
# dbExecute(connection, del)
# dbCommit(connection) # запись изменений в базу

# ДАННЫЕ С ВЫТЯЖНЫХ ТЕРМОМЕТРОВ
if (Pulling) {
  soiltfiles = list.files('predictors/meteo/raw/Tpg/') # директория с первичными данными
  nfiles = length(soiltfiles)
  start = match(22471, as.integer(substr(soiltfiles,4,8))) # если продолжаем импорт, меняем файл
  cat(paste0('Всего ',nfiles,' метеостанций с вытяжными термометрами'))
  
  fieldwidth = c(5,5,3,3,5,2,5,2,5,2,5,2,5,2,5,2,5,2,5,2,5,2,5,2,5,2,5,2)
  cnames = c('station_code','year','month','day','t2','q2','t5','q5','t10','q10',
             't15','q15','t20','q20','t40','q40','t60','q60','t80','q80','t120',
             'q120','t160','q160','t240','q240','t320','q320')
  depthes = c(2,5,10,15,20,40,60,80,120,160,240,320)
  qs = c(6,8,10,12,14,16,18,20,22,24,26,28)
  workdir = 'predictors/meteo/raw/Tpg/'
  nvalues = 12 # число глубин
  deviceType = 'p'
}

if (Savinov) {
  # ДАННЫЕ С ТЕРМОМЕТРОВ САВИНОВА
  soiltfiles = list.files('predictors/meteo/raw/Tpgks/')
  nfiles = length(soiltfiles)
  cat(paste0('Всего ',nfiles,' метеостанций с термометрами Савинова'))
  
  # разметка файла
  fieldwidth = c(5,5,3,3,5,3,5,3,5,3,5,3)
  cnames = c('station_code','year','month','day','t5','q5','t10','q10',
             't15','q15','t20','q20')
  
  depthes = c(5,10,15,20)
  qs = c(6,8,10,12)
  start = match(22217, as.integer(substr(soiltfiles,4,8))) # начинаем импорт с первого файла
  workdir = 'predictors/meteo/raw/Tpgks/'
  nvalues = 4
  deviceType = 's'
}


# импорт данных в базу (файлов)
for (i in start : nfiles) {

  rawfile = soiltfiles[i]
  rawt = read.fwf(paste0(workdir,rawfile),fieldwidth) # размечаем файл в Data Frame
  if (Pulling) rawt = rawt[-1,] # для вытяжных термометров первую строку убираем

  # всё в целочисленные значения
  for (j in 1 : ncol(rawt)) rawt[,j] = as.integer(rawt[,j])
  colnames(rawt) = cnames

  # импорт данных в базу 
  stationCode = rawt[1,1]
  cat(paste0('Метеостанция: ',stationCode,', ',i,' из ',nfiles,', импорт начат: ',Sys.time(),'\n'))

  # заносим очередной файл
  dbBegin(connection) # открывает сессию для транзакции
  for (r in 1 : nrow(rawt)) { # очередная строка
    year  = rawt[r,2]
    month = rawt[r,3]
    day   = rawt[r,4] 
    date = substr(ISOdate(year,month,day),1,10)
    # cat(paste0('Метеостанция: ',stationCode,' дата: ',date,'\n'))
    insert = list()

    for (c in 1 : nvalues) {
      if (rawt[r,c*2+4] != 9) {
        ins = paste0('INSERT INTO soil_temperature (station_id, date, depth,measure_type,',
                     't,quality) VALUES (',stationCode,',\'',date,'\',',depthes[c],',\'',
                     deviceType,'\',',rawt[r,c*2+3]/10,',',rawt[r,c*2+4],');')
        dbExecute(connection, ins)
      }
    }
  }
  dbCommit(connection) # заносим изменения в базу одной пачкой по всему файл
}

###### ----- Snow Cover ------
# МОЩНОСТЬ СНЕЖНОГО ПОКРОВА
connection <- dbConnect(RSQLite::SQLite(), "predictors/meteo/databases/soil_snow.sqlite")
# создаём структуру
crtable = 'CREATE TABLE IF NOT EXISTS snow_cover (ids integer PRIMARY KEY AUTOINCREMENT,
          station_id integer, date date, depth smallint, degree smallint, 
          q1 smallint, q2 smallint, q3 smallint);'
dbExecute(connection, crtable)

snowfiles = list.files('predictors/meteo/raw/Snow/')
nfiles = length(snowfiles)
cat(paste0('Всего ',nfiles,' метеостанций с мощностью снежного покрова'))

# разметка файла
fieldwidth = c(5,5,3,3,5,3,2,2,2)
cnames = c('station_code','year','month','day','depth','degree','q1','q2','q3')

# start = 1
start = match(30565, as.integer(substr(snowfiles,4,8))) # начинаем импорт с первого файла
workdir = 'predictors/meteo/raw/Snow/'

# Заносим данные в базу
for (i in start : nfiles) {
  rawfile = snowfiles[i]
  rawt = read.fwf(paste0(workdir,rawfile),fieldwidth) # размечаем файл в Data Frame

  # всё в целочисленные значения
  # for (j in 1 : ncol(rawt)) rawt[,j] = as.integer(rawt[,j])
  colnames(rawt) = cnames
  head(rawt)
  
  # импорт данных в базу 
  stationCode = rawt[1,1]
  cat(paste0('Метеостанция: ',stationCode,', ',i,' из ',nfiles,', импорт начат: ',Sys.time(),'\n'))
  
  # заносим очередной файл
  dbBegin(connection) # открывает сессию для транзакции
  for (r in 1 : nrow(rawt)) { # очередная строка
    year  = rawt[r,2]
    month = rawt[r,3]
    day   = rawt[r,4] 
    date = substr(ISOdate(year,month,day),1,10)
    depth = rawt[r,5]
    
    if (depth != 9999) {
        ins = paste0('INSERT INTO snow_cover (station_id,date,depth,degree,',
                     'q1,q2,q3) VALUES (',stationCode,',\'',date,'\',',depth,',',
                     rawt[r,6],',',rawt[r,7],',',rawt[r,8],',',rawt[r,9],');')
        dbExecute(connection, ins)
    }
  }
  dbCommit(connection) # заносим изменения в базу одной пачкой по всему файл
}

##### ---- SNOW ROUTES -----
# маршрутные снеговые съемки
connection <- dbConnect(RSQLite::SQLite(), "predictors/meteo/databases/soil_snow.sqlite")
# новая таблица в ту же базу

# создаём структуру
dbExecute(connection, 'DROP TABLE snow_route;') # удаляем старую таблицу

crtable = 'CREATE TABLE IF NOT EXISTS snow_route (ids integer PRIMARY KEY AUTOINCREMENT,
          station_id integer, date date, route_type smallint, degree_cover smallint, 
          degree_route smallint, crust_route smallint, depth smallint, depth_max smallint,
          depth_min smallint, density decimal(4,2), crust_depth smallint, watered_depth smallint,
          water_depth smallint, water_volume_snow smallint, water_volume_total smallint,
          snow_cover_pattern smallint, snow_pattern smallint);'
dbExecute(connection, crtable)

snowfiles = list.files('predictors/meteo/raw/SnMar/')
nfiles = length(snowfiles)
cat(paste0('Всего ',nfiles,' метеостанций с маршрутными съемками снежного покрова'))

# разметка файла
fieldwidth = c(5,7,3,2,3,3,3,3,5,5,5,5,4,4,3,5,5,2,2)
cnames = c('stationCode','year','month','routeType','day','degreeCover','degreeRoute',
           'crustRoute','depth','depthMax','depthMin','density','cresutDepth',
           'wateredDepth','waterDepth','waterVolSnow','waterVolTotal',
           'snowCoverPattern','snowPattern')

start = match(22217, as.integer(substr(soiltfiles,4,8))) # начинаем импорт с первого файла
workdir = 'predictors/meteo/raw/SnMar/'

# Заносим данные в базу
for (i in start : nfiles) { # очередной файлы
 
  rawfile = snowfiles[i]
  rawt = read.fwf(paste0(workdir,rawfile),fieldwidth) # размечаем файл в Data Frame
  
  # всё в целочисленные значения
  # for (j in 1 : ncol(rawt)) rawt[,j] = as.integer(rawt[,j])
  colnames(rawt) = cnames
  head(rawt)
  
  # импорт данных в базу 
  stationCode = rawt[1,1]
  cat(paste0('Метеостанция: ',stationCode,', ',i,' из ',nfiles,', импорт начат: ',Sys.time(),'\n'))
  
  # заносим очередной файл
  dbBegin(connection) # открывает сессию для транзакции
  for (r in 1 : nrow(rawt)) { # очередная строка
    year  = rawt[r,2]
    month = rawt[r,3]
    day   = rawt[r,5] 
    date = substr(ISOdate(year,month,day),1,10)
    depth = rawt[r,9]
    
    for (i in 1 : ncol(rawt)) rawt[,i] = str_squish(rawt[,i])
    
    # заменяет пустые значения на NA
    rawt[rawt == ""] <- NA
    
    # для значение depth заменяем NA на 9999
    rawt[is.na(rawt[,9]),9] = 9999
    
    # для проверки
    # write.table(rawt,'predictors/meteo/raw/21802.csv', sep = '\t')

    if (depth != 9999) {
      ins = paste0('INSERT INTO snow_route (station_id, date, route_type, degree_cover,
          degree_route, crust_route, depth, depth_max, depth_min, density, crust_depth,
          watered_depth, water_depth, water_volume_snow, water_volume_total,
          snow_cover_pattern, snow_pattern) VALUES (',stationCode,',\'',date,'\',',
                   rawt[r,4],',',rawt[r,6],',',rawt[r,7],',',rawt[r,8],',',depth,',',
                   rawt[r,10],',',rawt[r,11],',',rawt[r,12],',',rawt[r,13],',',
                   rawt[r,14],',',rawt[r,15],',',rawt[r,16],',',rawt[r,17],',',
                   rawt[r,18],',',rawt[r,19],');')
      ins = str_replace_all(ins,'NA','null')
      dbExecute(connection, ins)
    }
  }
  dbCommit(connection) # заносим изменения в базу одной пачкой по всему файл
}