# 2023-12-03
# этап 2 - сводка по метеостациям с данными почвенного климата
# суточные данные: температура и осадки, мощность снегового покрова,
# снеговые маршрутные съемки, температура почвы,
# температура почвы коленчатыми термометрами Савинова


setwd('/media/carabus/CARABUS20162/projects/EW_SDM/predictors/meteo/')

library(sf)
library(stringr)
library(ggplot2)

# список метеостанций GHCNd
# https://www.ncei.noaa.gov/pub/data/ghcn/daily/ghcnd-stations.txt
fieldwidth = c(11,9,10,7,4,31,8,6)
stations = read.fwf('../../vector/Weather_Stations/ghcnd-stations_2023-12-03.txt',
              widths = fieldwidth) 

colnames(stations) = c('station_code','latitude','longitude','altitude','province','station_name','fig','national_code')

stations$province = str_squish(stations$province)
stations$station_name = str_squish(stations$station_name)
stations$fig = str_squish(stations$fig)

stationsRU = str_subset(stations$station_code,'RSM')
length(stationsRU) # всего 1102 российкиих метеостанций в междунородной базе
stationsRUcode = as.integer(substr(stationsRU,7,11))
write.csv(stations, 'data/stations_ghcnd.csv')

# слой c метеостанциями по данным ВНИИГМИ-МЦД
stationsVniigmi = st_read('../../vector/Weather_Stations/stations_vniigmi.shp',
                         options = "ENCODING=UTF8")
colnames(stationsVniigmi) = c('code','name','latitude','longitude','altitude','geometry')


# СПИСКИ МЕТЕОСТАНЦИЙ
# температура воздуха-осадки
airTP = list.files('data/Air_Daily_Temperature_Precipitation/raw/')
# всего 600 станций
# надо просто расширение отбросить
airTP = as.integer(substr(airTP,1,5))
length(airTP) # 

intersect(airTP,stationsRUcode) # 503 - в международном списке не все российские
setdiff(airTP,stationsRUcode) # станции из вектора airTP, которых нет в stationsRUcode
setdiff(airTP,stationsVniigmi$code) # в российском списке всё есть - ВНИИГМИ-МЦД

# мощность снежного покрова
snowCover = list.files('data/Snow_Cover/raw/')
snowCover = as.integer(substr(snowCover,4,8))
length(snowCover) # 620 станций
setdiff(snowCover,stationsVniigmi$code) # в российском списке всё есть - ВНИИГМИ-МЦД
setdiff(snowCover, airTP)
setdiff(airTP, snowCover)
setdiff(snowCover,stationsRUcode)

# маршрутная снеговая съемка
snowRoute = as.integer(substr(list.files('data/Snow_Routes/raw/'),1,5))
length(snowRoute) # 517 станций
setdiff(snowRoute,snowCover)
setdiff(snowRoute,stationsVniigmi$code)
# три неизвестных станции
# 27331 (Ярославль) есть в списке: http://meteomaps.ru/meteostation_codes.html
# 28602 - неизвестно где
# 28612 (Муслюмова, Татарстан): https://meteo7.ru/station/28612

# почвенная температура
# список станций
soilT = list.files('data/Soil_Temperature/raw/')
soilT = as.integer(substr(soilT,4,8))
length(soilT) # 264 метеостанций
setdiff(soilT,stationsVniigmi$code) # всё есть согласно списку ВНИИГМИ

# почвенная температура - термометры Савинова
soilTS = list.files('data/Soil_Temperature_Savinov/raw/')
soilTS = as.integer(substr(soilTS,4,8))
length(soilTS) # 315 метеоатанций
setdiff(soilTS, stationsVniigmi$code)# всё есть согласно списку ВНИИГМИ
setdiff(soilT,soilTS) # разница в 55 станций
setdiff(soilTS,soilT) # разница в 106 станций

# список метеостанций по ВНИИГМИ-МЦД
write.csv(stationsVniigmi,'data/stations_vniigmi-mcd.csv')


# территория интереса
aoi = read_sf('../../vector/Area_of_Interest/Russia_European_albers.shp')

# проекция альберса
# в WGS84
aoi_dd = st_transform(aoi,st_crs('EPSG:4326'))

# схема
plot(st_geometry(stationsVniigmi), pch =16, col = 'blue')
plot(st_geometry(aoi_dd), lwd = 2, border = 'red', add = T)


# метеостанции внутри территории интереса
stations_aoi = st_filter(stationsVniigmi,aoi_dd) # всего 373 метеостанций
write.csv(stations_aoi,'data/stationsVniigmi_aoi.csv')

basemap = ggplot() +
  geom_sf(data = stationsVniigmi, col = 'blue', size = 0.5) +
  geom_sf(data = aoi_dd, lwd = 1.3, colour = 'red', fill = NA) + 
  coord_sf(xlim = c(28,66), ylim = c(45,70))

basemap +
  ggtitle('Метеостанции на территори интереса') 
  

# распределение метеостанций на территории интереса по ВНУМ
ggplot(data = stations_aoi) + 
  geom_histogram(aes(x = altitude)) +
  labs(x = 'ВНУМ, м', y = 'число метеостанций') +
  ggtitle('Распределение метеостанций по ВНУМ  на территории интереса')

# станции с 
stations_aoi_air = stations_aoi[stations_aoi$code %in% airTP,]
nrow(stations_aoi_air) # 143 станции

# мощность снежного покрова
stations_aoi_snc = stations_aoi[stations_aoi$code %in% snowCover,]
nrow(stations_aoi_snc) # 148 станции

# маршрутные снегосъёмки
stations_aoi_snr = stations_aoi[stations_aoi$code %in% snowRoute,]   
nrow(stations_aoi_snr) # 202 станции

# почвенные температуры
stations_aoi_soilt  = stations_aoi[stations_aoi$code %in% soilT,] 
nrow(stations_aoi_soilt) # 80 станций

stations_aoi_soilts = stations_aoi[stations_aoi$code %in% soilTS,]  
nrow(stations_aoi_soilts) # 104 станции


union(stations_aoi_soilt$code,stations_aoi_soilts$code) # всего 111

setdiff(stations_aoi_soilt$code,stations_aoi_soilts$code)
# 22249 22550 23219 23324 23418 28411 34866
setdiff(stations_aoi_soilts$code,stations_aoi_soilt$code)

basemap + 
  geom_sf(data = stations_aoi_air, color = 'navy') +
  coord_sf(xlim = c(28,66), ylim = c(45,70)) +
  ggtitle('Данные суточных температур и осадков')

basemap + 
  geom_sf(data = stations_aoi_snc, color = 'cyan') +
  coord_sf(xlim = c(28,66), ylim = c(45,70)) +
  ggtitle('Данные мощности снежного покрова')

basemap + 
  geom_sf(data = stations_aoi_soilt, color = 'orange') +
  coord_sf(xlim = c(28,66), ylim = c(45,70)) +
  ggtitle('Данные почвенной температуры')

basemap + 
  geom_sf(data = stations_aoi_soilts, color = 'deeppink') +
  coord_sf(xlim = c(28,66), ylim = c(45,70)) +
  ggtitle('термометры Савинова')
