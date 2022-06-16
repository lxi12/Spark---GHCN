## Analysis 4d
# Plot time series for TMIN and TMAX for stations in NZ
library(ggplot2)
library(dplyr)

nz <- read.csv('daily_avNZ/part-00000-b66bdaef-e066-44ad-98ed-baf6046bd9b3-c000.csv')

ggplot(nz, aes(x=YEAR, y=AVERAGE, group=paste0(ELEMENT, ID), color=ELEMENT)) +
  geom_line() + facet_wrap(~ID)+
  labs(title='Time series for TMIN/TMAX value by station in NZ')


# plot average TMIN/TMAX for NZ
nz_mean <- aggregate(AVERAGE ~ ELEMENT + YEAR, data=nz, mean)

ggplot(nz_mean, aes(YEAR, AVERAGE, group=ELEMENT, color=ELEMENT))+geom_line()+
  labs(title='Average time series for TMIN/TMAX value for NZ')



## Analysis 4e
# plot the average rainfall in 2021
library(ggplot2)
theme_set(theme_bw())
library(sf)
library(RColorBrewer)
library(rnaturalearth)
library(rnaturalearthdata)

rain <- read.csv('rainfall/part-00000-42e1c260-6492-4944-820d-2032f08a546f-c000.csv')
rain21 <- rain[rain$YEAR ==2021,]

world <- ne_countries(scale = "medium", returnclass = "sf")

data <- merge(world,rain21,by.x='name', by.y='NAME',all.x=T)
colnames(data)

data$value <- cut(data$avg.VALUE., breaks=seq(from=0, to=180, length.out=6))
ggplot(data = data) +
  geom_sf(aes(fill=value)) +
  scale_fill_brewer(name='Rainfall',palette="RdYlBu", na.value="lightgrey") +
  xlab("Longitude") + ylab("Latitude") +
  ggtitle("2021 Average Rainfall by Country")
  
#ggplot(data = data) +
#  geom_sf(aes(fill=avg.VALUE.)) +  
#  scale_fill_gradientn(name="Rainfall",
#                      colours=rev(brewer.pal(9,"Spectral")),
#                       na.value="lightgrey")+
#  xlab("Longitude") + ylab("Latitude") +
#  ggtitle("2021 Average Rainfall by Country")





 


 















