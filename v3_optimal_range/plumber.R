library(plumber)
library(uniswap)
library(jsonlite)
library(httr)

#* @apiTitle Uniswap v3 Optimal Range
#* @apiDescription Given a set of trades structured as Flipside Crypto ethereum.uniswapv3.ez_swaps
#* for a specific pool (e.g., ETH-WBTC 0.3\% fee tier on Ethereum mainnet) get the the optimal
#* range for accumulating 1 of the tokens in that pool (e.g., maximizing ETH).

#' @filter cors
cors <- function(req, res) {

  res$setHeader("Access-Control-Allow-Origin", "*")

  if (req$REQUEST_METHOD == "OPTIONS") {
    res$setHeader("Access-Control-Allow-Methods","*")
    res$setHeader("Access-Control-Allow-Headers", req$HTTP_ACCESS_CONTROL_REQUEST_HEADERS)
    res$status <- 200
    return(list())
  } else {
    plumber::forward()
  }

}


#* Echo back the input
#* @param msg The message to echo
#* @get /echo
function(msg = "") {
     list(msg = paste0("The message is: '", msg, "'"))
}

#* Parse JSON structured requests
#* @post /check_request
#*
function(req) {
  # Parse the JSON body
  # Return the parsed JSON body
  headers <- req$HEADERS
  body <- req$postBody
  return(
    body
    )
}

#* Get Save
#* @get /pull_save
function(){
  return(readRDS("save.rds"))
}

#* Return an Optimization
#*
#* @apiDescription Include in the http a body that contains A JSON of trades of {tick, amount0_adjusted, amount1_adjusted, liquidity} to optimize over given the parameters.
#*
#* @param budget The maximum amount of Token 1 available to distribute between Token 0 and Token 1 using Price 1, e.g., 100 ETH to allocate between ETH and BTC.
#* @param denominate The Token seeking to be maximized, 1 for Token 1, 0 for Token 0.
#* @param p1 The initial price used for allocating the budget, if NULL, use the first trade tick in trades table.
#* @param p2 The final price for assessing strategy value, if NULL, use the last trade tick in trades table.
#* @param decimal_x The decimal of Token 0, e.g., 1e18 for most ERC20, 1e8 for WBTC, 1e6 for USDC.
#* @param decimal_y The decimal of Token 1, e.g., 1e18 for WETH, etc.
#* @param fee The trade fee paid to in-range positions, typically one of 0.0001, 0.0005, 0.003, 0.01
#* @post /calc_optimum_range

function(req, budget = 100, denominate = 1, p1 = NULL, p2 = NULL, decimal_x = 1e18, decimal_y = 1e18, fee) {

trades = req$postBody
trades <- fromJSON(trades)
trades <- trades$params

budget <- as.numeric(budget)
denominate <- as.numeric(denominate)

p1 <- as.numeric(p1)
p2 <- as.numeric(p2)

if(length(p1) == 0 | p1 == '' | p1 == 0 | is.null(p1)){
  p1 <- NULL
}
if(length(p2) == 0 | p2 == '' | p2 == 0 | is.null(p2)){
  p2 <- NULL
}

decimal_x <- as.numeric(decimal_x)
decimal_y <- as.numeric(decimal_y)
fee <- as.numeric(fee)


  decimal_adjustment <- max(c(decimal_y/decimal_x, decimal_x/decimal_y))


  required_colnames <- c("tick","liquidity","amount0_adjusted","amount1_adjusted")

  if( mean( required_colnames %in% colnames(trades) ) != 1 ){
  stop("Need the following columns: tick, liquidity, amount0_adjusted, amount1_adjusted")
  }

 if(is.null(p1)){
   p1 <- tick_to_price(trades$tick[1], decimal_adjustment = decimal_adjustment)
 }
  if(is.null(p2)){
    p2 <- tick_to_price(tail(trades$tick,1), decimal_adjustment = decimal_adjustment)
  }


  paramz <- list(
    budget, denominate, p1, p2, decimal_x, decimal_y, fee
  )

  # Use naive search to get close-enough initial parameters for optimization
  low_price <- ((1:9)/10)*p1
  amount_1 <- c(1, budget*(1:9)/10)

  grid <- expand.grid(x = amount_1, y = low_price)

  sv <- lapply(1:nrow(grid), function(j){
    tryCatch({
      calculate_profit(
        params = c(grid[j,1], grid[j,2]),
        budget = budget, p1 = p1, p2 = p2, trades = trades,
        decimal_x = decimal_x, decimal_y = decimal_y, fee = fee,
        denominate = denominate,
        in_optim = TRUE)
    }, error = function(e){return(0)})
  })

  sv <- unlist(sv)

  # initialize using naive search min
  init_params <- as.numeric(grid[which.min(sv), 1:2])

  # lower_bounds(amount1 = 0.01 * budget, p1 = 0.09 * current price)
  # upper_bounds(amount1 = .99 * budget, p1 = 0.99 * current price)
  lower_bounds <- c(0.01*budget, 0.09*p1)
  upper_bounds <- c(.99*budget, 0.99*p1)

  # in_optim = TRUE provides *only* -1*strategy value for optimization
  # (-1 b/c algorithm looks for minimums and we want maximum)

  result <- optim(init_params,
                  calculate_profit,
                  method = "L-BFGS-B",
                  lower = lower_bounds,
                  upper = upper_bounds,
                  budget = budget, p1 = p1, p2 = p2, trades = trades,
                  decimal_x = decimal_x, decimal_y = decimal_y, fee = fee,
                  denominate = denominate, in_optim = TRUE)

  # in_optim = FALSE provides full audit of calculation
  profit = calculate_profit(params = result[[1]],
                            budget = budget, p1 = p1, p2 = p2, trades = trades,
                            decimal_x = decimal_x, decimal_y = decimal_y, fee = fee,
                            denominate = denominate,
                            in_optim = FALSE)

  # gmp bigz cannot be serialized for http returns
  profit$position$liquidity <- as.numeric(profit$position$liquidity)

  ret <- list(
    p1 = p1,
    p2 = p2,
    init_params = init_params,
    result_par = result$par,
    result_warn = result$message,
    position_details = profit$position,
    strategy_details = profit$strategy_value
  )

  saveRDS(list(paramz, trades, ret), "save.rds")

  return(ret)

}

# Programmatically alter your API
#* @plumber
function(pr) {
    pr %>%
        # Overwrite the default serializer to return unboxed JSON
        pr_set_serializer(serializer_unboxed_json())

}
