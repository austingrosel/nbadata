#
# This is a Shiny web application. You can run the application by clicking
# the 'Run App' button above.
#
# Find out more about building applications with Shiny here:
#
#    http://shiny.rstudio.com/
#

library(shiny)
library(DT)

# Define UI for application that draws a histogram
ui <- fluidPage(
   
   # Application title
   titlePanel("Old Faithful Geyser Data"),
   
   DTOutput('tbl')
)

# Define server logic required to draw a histogram
server <- function(input, output) {
  
  output$tbl = renderDT({
    df = final_df %>% filter(exp_fp > 15)
    df$diff = round(df$diff, 1)
    
    brks <- quantile(c(df$exp_fp, df$fp), probs = seq(0.05, 0.95, 0.1), na.rm = T)
    clrs <- round(seq(255, 40, length.out = length(brks) + 1), 0) %>% {paste0("rgb(", ., ",255,", ., ")")}
    
    diff_brks <- quantile(c(df$diff), probs = seq(0.05, 0.95, 0.1), na.rm = T)
    diff_clrs <- round(seq(255, 40, length.out = length(brks) + 1), 0) %>% {paste0("rgb(255,", ., ",", ., ")")}
    
    DT::datatable(df, rownames=F, options = list(searching = F, pageLength = 20)) %>%
      formatStyle(c('exp_fp', 'fp'), backgroundColor = styleInterval(brks, clrs)) %>%
      formatStyle(c('diff'), backgroundColor = styleInterval(diff_brks, diff_clrs))
  })
}

# Run the application 
shinyApp(ui = ui, server = server)

