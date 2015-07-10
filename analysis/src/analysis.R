library(dplyr)
library(ggplot2)

# Add updateid to out
addUpdateId <- function(out) {
  out = out %>%
    mutate(updateid = paste(docid, sentenceid, sep='-'))
  return(out)
}

# Chart doc score vs nugget score
docVsNugget <- function(out, i) {
  out %>% 
    merge(matches, by = 'updateid') %>% 
    merge(nuggets, by = 'nuggetid') %>% 
    group_by(score) %>% 
    summarize(meandoc = mean(docscore)) %>% 
    ggplot(aes(x = score, y = meandoc)) + 
      geom_line() + geom_point() +
      labs(title = paste0('Query ', i), 
           x = 'Nugget Score', y = 'Mean Doc Score') 
}

# Chart sentence score vs nugget score
sentVsNugget <- function(out, i) {
  out %>% 
    merge(matches, by = 'updateid') %>% 
    merge(nuggets, by = 'nuggetid') %>% 
    group_by(score) %>% 
    summarize(meansent = mean(sentencescore)) %>% 
    ggplot(aes(x = score, y = meansent)) + 
    geom_line() + geom_point() +
    labs(title = paste0('Query ', i), 
         x = 'Nugget Score', y = 'Mean Sentence Score') 
}

# Graph average sentence score by position (top 25 sentences) for each query
sentencePosAllQueries <- function() {
  for (i in 1:10) {
    if (i == 7) {
      next
    } else {
      out <- read.delim(paste0("~/@Garrick/School Work/Grad School/GSLIS/Summer 2015/TREC/Temporal Summarization/out.", i), header=FALSE, quote="", stringsAsFactors=FALSE)
      names(out) = headers
      print(sentencePos(out, i))
      rm(out)
      gc()
    }
  }
}

# Graph average sentence score by position (top 25 sentences)
sentencePos <- function(out, i) {
  out %>% 
    group_by(sentenceid) %>% 
    summarize(mean = mean(sentencescore)) %>% 
    filter(sentenceid < 25) %>% 
    ggplot(aes(x = sentenceid, y = mean)) +
      geom_point() + 
      geom_line() +
      labs(title = paste0('Query ', i), 
           x = 'Sentence Position', 
           y = 'Mean Score')
}

sentencePosNuggetAllQueries <- function(matches, nuggets) {
  for (i in 1:10) {
    if (i == 7) {
      next
    } else {
      out <- read.delim(paste0("~/@Garrick/School Work/Grad School/GSLIS/Summer 2015/TREC/Temporal Summarization/out.", i), header=FALSE, quote="", stringsAsFactors=FALSE)
      names(out) = headers
      out = addUpdateId(out)
      print(sentencePosNugget(out, matches, nuggets, i))
      rm(out)
      gc()
    }
  }
}

# Graph average nugget score by sentence position
sentencePosNugget <- function(out, matches, nuggets, i) {
  matches %>% 
    merge(out, by = 'updateid') %>% 
    merge(nuggets, by = 'nuggetid') %>% 
    group_by(sentenceid) %>% 
    summarize(mean = mean(score)) %>% 
    filter(sentenceid < 25) %>% 
    ggplot(aes(x = sentenceid, y = mean)) + 
    geom_point() + geom_line() +
    labs(title = paste0('Query ', i), 
         x = 'Sentence Position', 
         y = 'Mean Nugget Score')
}

# Graph average document score by time
avgDocScoreByTime <- function(out) {
  out.1 %>%
    group_by(epoch) %>%
    summarize(mean = mean(docscore)) %>%
    ggplot(aes(x = epoch, y = abs(mean))) +
    geom_bar(stat = 'identity')
      geom_point() +
      geom_line()
}

# Score of matches vs non-matches
# (non-matches over all sentences)
matchVsNonMatch <- function(out, matches, pooled, nuggets) {
  nm = out %>%
    subset(!(updateid %in% matches$updateid)) %>%
    summarize(meanDocScore = mean(docscore),
              meanSentScore = mean(sentencescore))
  nmp = out %>%
    subset(updateid %in% pooled$updateid) %>%
    subset(!(updateid %in% matches$updateid)) %>%
    summarize(meanDocScore = mean(docscore),
              meanSentScore = mean(sentencescore))
  m = matches %>% 
    merge(out, by = 'updateid') %>% 
    merge(nuggets, by = c('nuggetid', 'query')) %>% 
    select(nuggetid, query, docid,
           sentenceid, start, end,
           epoch.x, docscore, sentencescore,
           epoch.y, score) %>% 
    filter(score >= 0) %>%
    summarize(meanDocScore = mean(docscore),
              meanSentScore = mean(sentencescore))

  print('Matches:')
  print(m)
  print('Non-matches (pooled sentences):')
  print(nmp)
  print('Non-matches (all sentences):')
  print(nm)
}

# Graph true scores by time
docScoreByTimeNuggets <- function(matches, nuggets, query) {
  matches %>%
    merge(nuggets, c('query', 'nuggetid')) %>%
    filter(query == query, score >= 0) %>%
    group_by(epoch) %>%
    summarize(meanScore = mean(score)) %>%
    ggplot(aes(x = epoch, y = meanScore)) +
      geom_point() +
      labs(title = "Average Nugget Importance by Time", 
           y = 'Mean Score', x = 'Time')
}