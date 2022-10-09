

const ARTIST_INSIGHTS_QUERIES = QUERIES.QUERY_GET_ARTIST_INFO.ARTIST_INSIGHTS
const buildGetInsightsCountQuery = ARTIST_INSIGHTS_QUERIES.GET_INSIGHTS_COUNT
const buildGetArtistInsightsQuery = ARTIST_INSIGHTS_QUERIES.GET_ARTIST_INSIGHTS

function getInsightsCountFromDb({ id, highWeight, mediumWeight, daysAgo }){
    const query = buildGetInsightsCountQuery(id, highWeight, mediumWeight, daysAgo)
    return snowflakeClientExecuteQuery(query);
}

function getArtistInsightsFromDb({ id, limit, weight, daysAgo }){
    const query = buildGetArtistInsightsQuery(id, limit * 10, weight, daysAgo)
    return snowflakeClientExecuteQuery(query);
}

async function defineWeight({id, weight, daysAgo}){ // return a number
    const highWeight = 8;
    const mediumWeight = 4;
    const lowWeight = 1
    let definedWeight = weight
    if (isNaN(weight)) {
        const counts = await getInsightsCountFromDb({id, highWeight, mediumWeight, daysAgo})
        const [high, medium] = counts
        const [isHighWeight, isMediumWeight] = [high?.count, medium?.count]
        const lowOrMediumWeight = isMediumWeight ? mediumWeight : lowWeight
        definedWeight = isHighWeight ? highWeight : lowOrMediumWeight;
    }
    return definedWeight
}

function createInsights({formatResult, newsFormat, limit}){
    const initiaInsights = []
    return formatResult
        .slice(0) // create a copy of formatResult for iterating
        .reduce((accInsights, result, _id, arr) => {
            if(accInsights.length >= limit){
                arr.splice(0) // stop reduce by mutating iterated arr
            }else if(result){
                accInsights = [...accInsights, Boolean(newsFormat) ? insightToNews(result) : result]
            }
            return accInsights
        }, initiaInsights)
}

async function getArtistInsights({ id, limit, weight, daysAgo, newsFormat }) {
    try{
        const definedWeight = await defineWeight({id, weight, daysAgo})
        
        const sfResult = await getArtistInsightsFromDb({id, limit, definedWeight, daysAgo })
        const filteredResult = filterResults(sfResult)
        
        const formatResult = await Promise.all(
            filteredResult.map(result => formatInsight(result)) // formatInsight accepts an object and returns a Promise
        )

        const insightsLimit = Math.abs(limit + (10 - definedWeight) * 200) // TODO should we ensure definedWeight <= 10 ?
        const insights = createInsights({formatResult, newsFormat, limit: insightsLimit})

        return { insights, ...(Boolean(newsFormat) && {weight: definedWeight}) };

    }catch(err){
        // TODO handle err here
    }
}