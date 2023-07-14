library(plumber)
library(uniswap)
library(jsonlite)
library(httr)
library(shroomDK)

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

#* Process from Svelte UI
#* Calls /calc_optimum_range inside Snowflake SQL.
#* @param budget The maximum amount of Token 1 available to distribute between Token 0 and Token 1 using Price 1, e.g., 100 ETH to allocate between ETH and BTC.
#* @param denominate The Token seeking to be maximized, 1 for Token 1, 0 for Token 0.
#* @param from_block Initial block to get WBTC-ETH Trades for analysis.
#* @param to_block Final block to get WBTC-ETH Trades for analysis.
#* @post /bdft
function(budget, denominate, from_block, to_block){
budget <- as.numeric(budget)
denominate <- as.numeric(denominate)
from_block <- as.numeric(from_block)
to_block <- as.numeric(to_block)

  query <- {
    "
with inputs AS (
     SELECT
            LOWER('0xCBCdF9626bC03E24f779434178A73a0B4bad62eD') as contract_address,
            __FROM_BLOCK__ as from_block,
            concat('0x',trim(to_char(from_block,'XXXXXXXXXX'))) as hex_block_from,
            __TO_BLOCK__ as to_block,
            concat('0x',trim(to_char(to_block,'XXXXXXXXXX'))) as hex_block_to,
            ARRAY_CONSTRUCT(
            '0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67',
            NULL,
            NULL
            )  as event_topic_param
),


pool_details AS (
  select POOL_NAME,
TOKEN1_SYMBOL,
TOKEN1_DECIMALS,
TOKEN0_SYMBOL,
TOKEN0_DECIMALS,
ABS(TOKEN1_DECIMALS - TOKEN0_DECIMALS) as decimal_adjustment
from ethereum.core.dim_dex_liquidity_pools
where pool_address = (select lower(contract_address) from inputs)
 ),

create_rpc_request as (
SELECT
    contract_address,
    from_block,
    to_block,
     livequery.utils.udf_json_rpc_call(
            'eth_getLogs',
            [{ 'address': contract_address,
'fromBlock': hex_block_from,
'toBlock': hex_block_to,
'topics': event_topic_param }]
     ) AS rpc_request
FROM
     inputs),

 base AS (
         SELECT
         livequery.live.udf_api(
                   'POST', -- method
                   '{eth-mainnet-url}', -- url
                    {},  -- default header
                   rpc_request, -- data
                   'charlie-quicknode' -- my registered secret name
            ) AS api_call
     FROM
            create_rpc_request
),

res AS (
SELECT
t.value:transactionHash::string as tx_hash,
t.value:address::string as address,
t.value:blockNumber::string as block_number,
regexp_substr_all(SUBSTR(t.value:data, 3, len(t.value:data)), '.{64}') as data
 from base,
LATERAL FLATTEN(input => api_call:data:result) t
),

send AS (
SELECT
address as pool_address,
tx_hash,
ethereum.public.udf_hex_to_int(block_number) as block_number,
ethereum.public.udf_hex_to_int('s2c',data[0]::STRING)::FLOAT/POW(10,(select TOKEN0_DECIMALS from pool_details)) as amount0_adjusted,
ethereum.public.udf_hex_to_int('s2c',data[1]::STRING)::FLOAT/POW(10,(select TOKEN1_DECIMALS from pool_details)) as amount1_adjusted,
ethereum.public.udf_hex_to_int(data[2]) as sqrtPX96,
POWER(sqrtPX96 / POWER(2, 96), 2)/(POWER(10, (SELECT decimal_adjustment from pool_details))) as price,
ethereum.public.udf_hex_to_int(data[3]) as liquidity,
ethereum.public.udf_hex_to_int(data[4]) as tick
FROM res
),

send_json AS (
SELECT '[' || LISTAGG(
         '{' ||
           '\"tick\": ' || tick || ',' ||
           '\"amount0_adjusted\": ' || amount0_adjusted || ',' ||
           '\"amount1_adjusted\": ' || amount1_adjusted || ',' ||
           '\"liquidity\": ' || liquidity::integer || '' ||
         '}', ','
       ) || ']' AS trades_json
FROM send
),

 parameters AS (
  SELECT 'budget' AS key, __BUDGET__ AS value
  UNION ALL
SELECT 'denominate' AS key, __DENOMINATE__ AS value
  UNION ALL
  SELECT 'p1' AS key, 0 AS value -- 0 treated as NULL
  UNION ALL
  SELECT 'p2' AS key, 0 AS value -- 0 treated as NULL
  UNION ALL
  SELECT 'decimal_x' AS key, 100000000 AS value
  UNION ALL
  SELECT 'decimal_y' AS key, 1000000000000000000 AS value
  UNION ALL
  SELECT 'fee' AS key, 0.003 AS value
),

parameter_url AS (
 SELECT
 'https://science.flipsidecrypto.xyz/v3_optimal_range/calc_optimum_range?'
|| LISTAGG(key || '=' || value, '&') WITHIN GROUP (ORDER BY key) AS parameter_url
FROM
  parameters
),

body AS (
select livequery.utils.udf_json_rpc_call(
'',
(select PARSE_JSON(trades_json) from send_json)
) as req_body
)

 SELECT
 livequery.live.udf_api(
  'POST', -- method
  (select parameter_url from parameter_url),
  {'key': 'string'}, -- default header
  (select req_body from body) -- data
  ) AS api_call
     FROM DUAL
  "
  }
  query <- gsub(pattern = '__BUDGET__', replacement = budget, fixed = TRUE, x = query)
  query <- gsub(pattern = '__DENOMINATE__', replacement = denominate, fixed = TRUE, x = query)
  query <- gsub(pattern =  '__FROM_BLOCK__', replacement = from_block, fixed = TRUE, x = query)
  query <- gsub(pattern =  '__TO_BLOCK__', replacement = to_block, fixed = TRUE, x = query)

  x = shroomDK::auto_paginate_query(query, api_key = readLines("api_key.txt"))
  return(x)
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
