# sg-events

This project contains two unique Azure functions. The sg-event-activity-api function will poll for the most recent 1000 SendGrid mailing events on a 5 minute timer. 
The sg-events-webhook function will respond to the SendGrid event webhook when it receives an event. Both functions will update a unique CSV file (sgEventData.csv
and sgEventDataHOOK.csv respectively) and store them to a blob container.

The environment variables that follow will need to be added as an application setting in function app --> configuration. 
sgeventactivity_BEARER contains the SendGrid account's bearer token.
sgeventactivity_STORAGE contains the Azure blob storage account.
