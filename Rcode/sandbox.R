if (!("pacman" %in% rownames(utils::installed.packages()))) {
  utils::install.packages("pacman")
}
library(pacman)
p_load(dplyr, magrittr, knitr, ggplot2, stargazer, grid, gridExtra, beepr, devtools, tidyverse, rlang, arrow, here, cartogram, maptools, sf)
devtools::install_github("UrbanInstitute/urbnmapr")
library(urbnmapr)


### Load Geographic Data (Shapefiles)
camap <- read_sf(unzip(here('data','basemaps/us_county.zip'))[4]) %>% 
  filter(STATEFP == '06')

# camap %>% ggplot()+ geom_sf(aes(fill=ALAND))

# statemap <- read_sf(unzip(here('data','basemaps', 'us_state.zip'))[4])
map_projection <- st_transform(camap, crs=3310)


label_fun <- function(breaks) {
  labels <- breaks / 1000000
  return(labels)
}
y <- 2016
generate_cart_data <- function(y) {
  
  df <- read_csv(here('data', 'clean', 'year_county_data.csv')) %>% 
    rename('GEOID' = 'FIP') %>%
    filter(grepl('^06', GEOID) & year %in% (y)) %>%
    mutate(customer = ifelse(is.na(customer), 0, customer))  
    
  payment_data <- left_join(map_projection, df, by="GEOID") %>% 
    mutate(customer = ifelse(is.na(customer), 0, customer)) %>% 
    filter(!is.na(year))
    
  start.time <- Sys.time()
  cart.map <-  cartogram_cont(payment_data, weight="customer", prepare="none", itermax=15)
  end.time <- Sys.time()
  print('cartogram timer:')
  print(end.time - start.time)
  cart.map
}

years <-  2017:2019
cart_data <-  map(years, generate_cart_data)
cart_data <- bind_rows(cart_data, .id="column_label") 

generate_map <- function(cart_data, y=NULL){
  
  ggplot() +
    geom_sf(data = cart_data, aes(fill=payment)) + 
    scale_fill_gradientn(
      colours = hcl.colors(3, "GnBu", rev = TRUE),
      labels = label_fun,
      n.breaks = 10,
      guide = guide_colorsteps(
        barwidth = 20,
        barheight = 0.5,
        title = "million ($)",
        title.position = "right",
        title.vjust = 0.1)) +
    theme_void() +
    facet_wrap(~year, ncol=3) + 
    theme(legend.position = 'top',
          legend.text = element_text(angle = 45, 
                                     margin = margin(t=7)),
          plot.margin=(margin(0,0,0,0)),
          strip.text.x = element_text(face="bold", size=12),
          plot.title = element_text(face="bold", size=15, hjust=0.5, margin = margin(b=20))) +
    labs(title = "Sized by # of Participants, Fill By Payment($)")
}


generate_map(cart_data)


# map.1 <- generate_map(years[[1]], cart_data[[1]])
# map.2 <- generate_map(years[[2]], cart_data[[2]])
# map.3 <- generate_map(years[[3]], cart_data[[3]])
# 
# g <- grid.arrange(map.1, map.2, map.3, ncol=3, nrow=1, 
#              top = textGrob("Sized by # of Participants, Fill by Payment ($)",gp=gpar(fontsize=15,fontface='bold')))
# g

ggsave(here('output', 'maps', 'cartogram', 'poster_test_noprep.png'))
