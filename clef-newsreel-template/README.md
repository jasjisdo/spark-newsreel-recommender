# JAVA IMPLEMENTATION FOR THE CLEF CHALLENGE

This is a short description of the *Java* implementation for the *News Recommender Systems* challenge hosted by *plista*. We outline the essential components. Additionally, we emphasise on how to incorporate your own recommender.

## OVERVIEW CLASSES

+ Client.java
+ ContestHandler.java
+ RecommendedItem.java
+ RecommenderItemTable.java
+ DirtyRingBuffer.java

## COMMUNICATION WITH THE PLISTA CHALLENGE SERVER

The communication is handled by *Client.java* and *ContestHandler.java*. The *Client.java* receives the messages and forwards their contents to the *ContestHandler.java* who delegates the messages to the suited methods. There are four basic types of messages:

+ Item updates
+ Recommendation Request
+ Notifications (impressions, clicks)
+ Errors

Those message types are encoded as HTTP-POST-Parameter. The *Client.java* extracts the type information. Subsequently, the *ContestHandler.java* initiates the pre-defined way to handle the request.

## RECOMMENDER BASELINE

The implementation includes a baseline recommender. Considering recency as the most important factor represents the basic idea of the baseline. Each publisher (identified by *context.simple.27*) is assigned a list of fixed size. As new items arrive - either by updates/creates or through notifications (clicks/impressions) - the item Ids (identified by *context.simple.25*) are added to the list. Upon incoming recommendation requests, the *DirtyRingBuffer.java* returns the *N* most recent items. *N* refers to the number of items requested. As soon as the list capacity is exceeded, the least recent items are dropped.

## ITEM REPRESENTATION

We represent items as instances of *RecommenderItem.java*. Those instances provide access to data encoded in the JSON. When creating the *RecommenderItem.java*, the JSON is parsed and essential information such as item Id, publisher Id, and timestamp. The *RecommenderItem.java* instances are subsequently gathered in an array representation - *RecommenderItemTable.java*.

## REQUIRED ADAPTIONS

If you want to use this template to write your own recommender, you can use the item representations. Both *Client.java* and *ContestHandler.java* offer the opportunity to include a recommender. Those recommenders must return a *List<Long>* in order to create an adequate response. Note that you have to adjust the *properties* file to support the port used to communicate with the server. Further information can be found in <http://orp.plista.com>.

## Licence

MIT License, (c) 2014, TU Berlin