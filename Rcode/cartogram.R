if (!("pacman" %in% rownames(utils::installed.packages()))) {
  utils::install.packages("pacman")
}
library(pacman)
p_load(dplyr, magrittr, knitr, ggplot2, stargazer, beepr, devtools, tidyverse, rlang, arrow, here, cartogram, maptools, sf)
devtools::install_github("UrbanInstitute/urbnmapr")
library(urbnmapr)


### Load Geographic Data (Shapefiles)
# countymap <- read_sf(unzip(here('data','basemaps/us_county.zip'))[4])
statemap <- read_sf(unzip(here('data','basemaps', 'us_state.zip'))[4])
map_projection <- st_transform(statemap, crs=3310)

label_fun <- function(breaks) {
  labels <- breaks / 1000000
  return(labels)
}


make_cartogram <- function(y){
  print(y)
  ## Load Feature Data
  df <- read_csv(here('data', 'clean', 'year_state_data.csv')) %>% 
    rename('GEOID' = 'statecode') %>%
    filter(year == y) %>% 
    mutate(customer = ifelse(is.na(customer), 0, customer))
  
  
  payment_data <- left_join(map_projection, df, by="GEOID")
  
  start.time <- Sys.time()
  cart.map <-  cartogram_cont(payment_data, weight="customer", itermax=5)
  end.time <- Sys.time()
  print('cartogram timer:')
  print(end.time - start.time)
  
  
  cart.map %>%
    ggplot() +
    geom_sf(aes(fill=payment)) +
    coord_sf(xlim=c(-125, -66), ylim = c(25, 53), crs=4326) +
    scale_fill_gradientn(
      colours = hcl.colors(3, "GnBu", rev = TRUE),
      labels = label_fun,
      n.breaks = 15,
      guide = guide_colorsteps(
        barwidth = 20,
        barheight = 0.5,
        title = "million",
        title.position = "right",
        title.vjust = 0.1)) +
    theme_void() +
    theme(legend.position = 'top',
          legend.text = element_text(angle = 45, 
                                     margin = margin(t=7))) +
    labs(title = "Cartogram weighted by number of customers",
         subtitle = y)
  
  fname <- paste0('state_', y, '.png')
  ggsave(here('output', 'maps', 'cartogram', fname))
}

years <- 2006:2022
map(years, make_cartogram)

