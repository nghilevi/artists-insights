

const ARTIST_INSIGHTS_QUERIES = QUERIES.QUERY_GET_ARTIST_INFO.ARTIST_INSIGHTS;
const buildGetInsightsCountQuery = ARTIST_INSIGHTS_QUERIES.GET_INSIGHTS_COUNT;
const buildGetArtistInsightsQuery = ARTIST_INSIGHTS_QUERIES.GET_ARTIST_INSIGHTS;

function getInsightsCountFromDb({ id, highWeight, mediumWeight, daysAgo }){
    const query = buildGetInsightsCountQuery(id, highWeight, mediumWeight, daysAgo);
    return snowflakeClientExecuteQuery(query);
}

function getArtistInsightsFromDb({ id, limit, weight, daysAgo }){
    const query = buildGetArtistInsightsQuery(id, limit * 10, weight, daysAgo);
    return snowflakeClientExecuteQuery(query);
}

async function evaluateWeight ({id, daysAgo}) {
    const weight = {high: 8, medium: 4, low: 1};
    const counts = await getInsightsCountFromDb({id, highWeight: weight.high, mediumWeight: weight.medium, daysAgo});
    const [isHigh, isMedium] = [counts[0]?.count, counts[1]?.count];
    return isHigh ? weight.high : (isMedium ? weight.medium : weight.low);
}

async function defineWeight({id, weight, daysAgo}){ // return a number
    return typeof weight === 'number' ? weight : await evaluateWeight({id, daysAgo});
}

function createInsights({formattedInsights, isNewsFormat, limit}){
    const insights = [];
    let i = 0;
    while(insights.length < limit && i < formattedInsights.length){
        const insight = formattedInsights[i]
        if(insight){
            insights.push(isNewsFormat ? insightToNews(insight) : insight)
        }
        i++;
    }
    return insights
}

async function getArtistInsights({ id, limit, weight, daysAgo, newsFormat }) {
    try{
        const definedWeight = await defineWeight({id, weight, daysAgo});
        const sfResult = await getArtistInsightsFromDb({id, limit, definedWeight, daysAgo });
        const filteredResults = filterResults(sfResult);
        const formattedInsights = await Promise.all(
            filteredResults.map(result => formatInsight(result)) // formatInsight accepts an object and returns a Promise
        );
        const insightsLimit = Math.abs(limit + (10 - definedWeight) * 200); // TODO should we ensure definedWeight <= 10 ?
        const isNewsFormat = Boolean(newsFormat)
        const insights = createInsights({formattedInsights, isNewsFormat, limit: insightsLimit});
        return { insights, ...(isNewsFormat && {weight: definedWeight}) };
    }catch(err){
        // TODO handle err here
    }
}