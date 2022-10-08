const highWeight = 8;
const mediumWeight = 4;
const lowWeight = 1

const ARTIST_INSIGHTS_QUERIES = QUERIES.QUERY_GET_ARTIST_INFO.ARTIST_INSIGHTS
const buildGetInsightsCountQuery = ARTIST_INSIGHTS_QUERIES.GET_INSIGHTS_COUNT
const buildGetArtistInsightsQuery = ARTIST_INSIGHTS_QUERIES.GET_ARTIST_INSIGHTS

function getInsightsCountFromDb({ id, daysAgo }){
    const query = buildGetInsightsCountQuery(id, highWeight, mediumWeight, daysAgo)
    return snowflakeClientExecuteQuery(query);
}

function getArtistInsightsFromDb({ id, limit, weight, daysAgo }){
    const query = buildGetArtistInsightsQuery(id, limit * 10, weight, daysAgo)
    return snowflakeClientExecuteQuery(query);
}

function getInsightsCount({ id, daysAgo, weight }){
    const countPromise = isNaN(weight) ? getInsightsCountFromDb({id, daysAgo}) : Promise.resolve();
    return countPromise
}

function defineWeight({ weight, counts }){ // TODO getInsightsCount here? / refactor control flow
    let definedWeight = weight
    if (isNaN(definedWeight)) {
        const [high, medium] = counts
        const [isHighWeight, isMediumWeight] = [high?.count, medium?.count]
        const lowOrMediumWeight = isMediumWeight ? mediumWeight : lowWeight
        definedWeight = isHighWeight ? highWeight : lowOrMediumWeight;
    }
    return definedWeight
}

function getArtistInsights({ id, limit, weight, daysAgo, newsFormat }) {

    const countPromise = getInsightsCount({id, weight, daysAgo})

    return countPromise.then(counts => {

        const definedWeight = defineWeight({ weight, counts })

        return getArtistInsightsFromDb({id, limit, definedWeight, daysAgo })
            .then(sfResult => filterResults(sfResult))
            .then(filteredResult => Promise.all(
                filteredResult.map(result => formatInsight(result)))
            )
            .then(results => {
                const endResult = limit + (10 - definedWeight) * 200
                return results.filter(e => e).slice(0, endResult); // filter empty values and get the first endResult
            })
            .then(results => Promise.all(
                results.map(result => Boolean(newsFormat) ? insightToNews(result) : result)
            ))
            .then(insights => {
                return { ...insights, ...(Boolean(newsFormat) && {weight: definedWeight}) };
            });

    });
}