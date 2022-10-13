'use strict';

const ARTIST_INSIGHTS_QUERIES = QUERIES.QUERY_GET_ARTIST_INFO.ARTIST_INSIGHTS;
const buildGetInsightsCountQuery = ARTIST_INSIGHTS_QUERIES.GET_INSIGHTS_COUNT;
const buildGetArtistInsightsQuery = ARTIST_INSIGHTS_QUERIES.GET_ARTIST_INSIGHTS;

const getInsightsCountFromDb = function _getInsightsCountFromDb({ id, highWeight, mediumWeight, daysAgo }){
    const query = buildGetInsightsCountQuery(id, highWeight, mediumWeight, daysAgo);
    return snowflakeClientExecuteQuery(query);
}

const getArtistInsightsFromDb = function _getArtistInsightsFromDb({ id, limit, weight, daysAgo }){
    const query = buildGetArtistInsightsQuery(id, limit * 10, weight, daysAgo);
    return snowflakeClientExecuteQuery(query);
}

const evaluateWeight = async function _evaluateWeight ({id, daysAgo}) {
    const weight = {high: 8, medium: 4, low: 1};
    const counts = await getInsightsCountFromDb({id, highWeight: weight.high, mediumWeight: weight.medium, daysAgo});
    const [isHigh, isMedium] = [counts[0]?.count > 0, counts[1]?.count > 0];
    return isHigh ? weight.high : (isMedium ? weight.medium : weight.low);
}

const defineWeight = async function _defineWeight({id, weight, daysAgo}){ // return a number
    return weight === weight && typeof weight === 'number' ? weight : await evaluateWeight({id, daysAgo});
}

const createInsights = function _createInsights({formattedInsights, isNewsFormat, limit}){
    const insights = [];
    let i = 0;
    while(insights.length < limit && i < formattedInsights.length){ // reason using while-loop: https://hackernoon.com/3-javascript-performance-mistakes-you-should-stop-doing-ebf84b9de951
        const insight = formattedInsights[i];
        if(insight) insights.push(isNewsFormat ? insightToNews(insight) : insight);
        i++;
    }
    return insights;
}

const getArtistInsights = async function _getArtistInsights({ id, limit, weight, daysAgo, newsFormat }) { // assume that id, limit, weight, daysAgo, newsFormat r in correct type
    try{
        const definedWeight = await defineWeight({id, weight, daysAgo});
        const sfResult = await getArtistInsightsFromDb({id, limit, definedWeight, daysAgo });
        const filteredResults = filterResults(sfResult);

        /* make sure the param array length does not exceed Promise.all limit
        src: https://stackoverflow.com/questions/55753746/how-much-is-the-limit-of-promise-all */
        const formattedInsights = await Promise.all( filteredResults.map(result => formatInsight(result)) ); // formatInsight accepts an object and returns a Promise

        const insightsLimit = Math.abs(limit + (10 - definedWeight) * 200); // assume insightsLimit is a positive value. TODO should we ensure definedWeight <= 10 ?
        const isNewsFormat = Boolean(newsFormat);
        const insights = createInsights({formattedInsights, isNewsFormat, limit: insightsLimit});
        return { insights, ...(isNewsFormat && {weight: definedWeight}) }; // always return an obj
    }catch(err){
        // TODO handle err here
    }
}