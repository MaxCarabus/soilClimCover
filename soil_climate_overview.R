# 2023-12-03
# этап 2 - сводка по метеостациям с данными почвенного климата
# суточные данные: температура и осадки, мощность снегового покрова,
# снеговые маршрутные съемки, температура почвы,
# температура почвы коленчатыми термометрами Савинова


setwd('/media/carabus/Enterprise/EW_SDM/predictors/meteo/')

library(sf)
library(stringr)
library(ggplot2)

# список метеостанций GHCNd - National Centers for Environmental Information
# https://www.ncei.noaa.gov/pub/data/ghcn/daily/ghcnd-stations.txt
# в скачанном файле заменили знак # на №

t = read.csv('weather_stations/ghcnd-stations.csv')
t$X = NULL
write.csv(t, 'weather_stations/ghcnd-stations.csv', row.names = F)

fieldwidth = c(11,9,10,7,4,31,8,6)
stations = read.fwf('weather_stations/ghcnd-stations.txt',
              widths = fieldwidth) 
colnames(stations) = c('station_code','latitude','longitude','altitude','province','station_name','fig','national_code')
head(stations)


stations$province = str_squish(stations$province)
stations$station_name = str_squish(stations$station_name)
stations$fig = str_squish(stations$fig)

stationsRU = str_subset(stations$station_code,'RSM') # коды станций - списком
length(stationsRU) # всего 1102 российкиих метеостанций в междунородной базе
stationsRUcode = as.integer(substr(stationsRU,7,11))
# write.csv(stations, 'weather_stations/stations_ghcnd.csv')

# слой c метеостанциями по данным ВНИИГМИ-МЦД
stationsVniigmi = st_read('weather_stations/stations_vniigmi.shp',
                         options = "ENCODING=UTF8")
colnames(stationsVniigmi) = c('code','name','latitude','longitude','altitude','geometry')


# СПИСКИ МЕТЕОСТАНЦИЙ
# температура воздуха-осадки
airTP = list.files('raw/Tttr/')
# всего 600 станций
# надо просто расширение отбросить
airTP = as.integer(substr(airTP,1,5))
length(airTP) # 

intersect(airTP,stationsRUcode) # 503 - в международном списке не все российские
setdiff(airTP,stationsRUcode) # станции из вектора airTP, которых нет в stationsRUcode
setdiff(airTP,stationsVniigmi$code) # в российском списке всё есть - ВНИИГМИ-МЦД

# мощность снежного покрова
snowCover = list.files('raw/Snow/')
snowCover = as.integer(substr(snowCover,4,8))
length(snowCover) # 620 станций
setdiff(snowCover,stationsVniigmi$code) # в российском списке всё есть - ВНИИГМИ-МЦД
setdiff(snowCover, airTP)
setdiff(airTP, snowCover)
setdiff(snowCover,stationsRUcode)

# маршрутная снеговая съемка
snowRoute = as.integer(substr(list.files('raw/SnMar/'),1,5))
length(snowRoute) # 517 станций
setdiff(snowRoute,snowCover)
setdiff(snowRoute,stationsVniigmi$code)
# три неизвестных станции
# 27331 (Ярославль) есть в списке: http://meteomaps.ru/meteostation_codes.html
# есть эта же метеостанция с кодом 27330

# 28602 - Чистополь
# есть эта же метеостанция с кодом 28601

# 28612 (Муслюмово, Татарстан): https://meteo7.ru/station/28612
# есть эта же метеостация с кодом 28611

# почвенная температура
# список станций
soilT = list.files('raw/Tpg/')
soilT = as.integer(substr(soilT,4,8))
length(soilT) # 264 метеостанций
setdiff(soilT,stationsVniigmi$code) # всё есть согласно списку ВНИИГМИ

# почвенная температура - термометры Савинова
soilTS = list.files('raw/Tpgks/')
soilTS = as.integer(substr(soilTS,4,8))
length(soilTS) # 315 метеоатанций
setdiff(soilTS, stationsVniigmi$code)# всё есть согласно списку ВНИИГМИ
setdiff(soilT,soilTS) # разница в 55 станций
setdiff(soilTS,soilT) # разница в 106 станций

# список метеостанций по ВНИИГМИ-МЦД
write.csv(stationsVniigmi,'weather_stations/stations_vniigmi-mcd.csv')


# территория интереса
# проекция альберса
aoi = read_sf('../../navigation/Russia_European_albers.shp')

# сохраняем территорию интереса в виде KMZ файла
st_write(aoi, dsn = '../../navigation/aoi_albers.kml', driver = 'KML')

# в WGS84
aoi_dd = st_transform(aoi,st_crs('EPSG:4326'))
st_write(aoi_dd, dsn = '../../navigation/aoi_dd.kml')

# схема
plot(st_geometry(stationsVniigmi), pch =16, col = 'blue')
plot(st_geometry(aoi_dd), lwd = 2, border = 'red', add = T)


# метеостанции внутри территории интереса
stations_aoi = st_filter(stationsVniigmi,aoi_dd) # всего 373 метеостанций
write.csv(stations_aoi,'weather_stations/stationsVniigmi_aoi.csv')

basemap = ggplot() +
  geom_sf(data = stationsVniigmi, col = 'blue', size = 0.5) +
  geom_sf(data = aoi_dd, lwd = 1.3, colour = 'red', fill = NA) + 
  coord_sf(xlim = c(28,66), ylim = c(45,70))

basemap +
  ggtitle('Метеостанции на территории интереса') +
  theme(plot.title = element_text(hjust = 0.5, size = 16))
  
# распределение метеостанций на территории интереса по ВНУМ
ggplot(data = stations_aoi) + 
  geom_histogram(aes(x = altitude)) +
  labs(x = 'ВНУМ, м', y = 'число метеостанций') +
  ggtitle('Распределение метеостанций по ВНУМ  на территории интереса') +
  theme(plot.title = element_text(hjust = 0.5, size = 16))

# станции по массиву данных с температурой воздуха и осадками 
stations_aoi_air = stations_aoi[stations_aoi$code %in% airTP,]
nrow(stations_aoi_air) # 143 станции

# мощность снежного покрова
stations_aoi_snc = stations_aoi[stations_aoi$code %in% snowCover,]
nrow(stations_aoi_snc) # 148 станции

# маршрутные снегосъёмки
stations_aoi_snr = stations_aoi[stations_aoi$code %in% snowRoute,]   
nrow(stations_aoi_snr) # 202 станции

# почвенные температуры по вытяжным термометрам
stations_aoi_soilt  = stations_aoi[stations_aoi$code %in% soilT,] 
nrow(stations_aoi_soilt) # 80 станций

# почвенные температуры по коленчатым термометрам Савинова
stations_aoi_soilts = stations_aoi[stations_aoi$code %in% soilTS,]  
nrow(stations_aoi_soilts) # 104 станции

union(stations_aoi_soilt$code,stations_aoi_soilts$code) # всего 111

setdiff(stations_aoi_soilt$code,stations_aoi_soilts$code)
# 22249 22550 23219 23324 23418 28411 34866
setdiff(stations_aoi_soilts$code,stations_aoi_soilt$code)

basemap + 
  geom_sf(data = stations_aoi_air, color = 'navy') +
  coord_sf(xlim = c(28,66), ylim = c(45,70)) +
  ggtitle('Данные суточных температур и осадков') +
  theme(plot.title = element_text(hjust = 0.5, size = 16))

basemap + 
  geom_sf(data = stations_aoi_snc, color = 'cyan') +
  coord_sf(xlim = c(28,66), ylim = c(45,70)) +
  ggtitle('Данные мощности снежного покрова') +
  theme(plot.title = element_text(hjust = 0.5, size = 16))

basemap + 
  geom_sf(data = stations_aoi_soilt, color = 'orange') +
  coord_sf(xlim = c(28,66), ylim = c(45,70)) +
  ggtitle('Данные почвенной температуры') + 
  theme(plot.title = element_text(hjust = 0.5, size = 16))

basemap + 
  geom_sf(data = stations_aoi_soilts, color = 'deeppink') +
  coord_sf(xlim = c(28,66), ylim = c(45,70)) +
  ggtitle('термометры Савинова') +
  theme(plot.title = element_text(hjust = 0.5, size = 16))