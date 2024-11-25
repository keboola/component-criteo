# Criteo Extractor
This extractor allows users to extract data reports from the Criteo Marketing Solutions API.

**Table of contents:**

[TOC]

## Getting credentials

Criteo oAuth flow requires client ids and secrets from a developer app. Since a developer app in criteo is universal, meaning it
can fetch data from all clients who authorize the app using a consent screen, the component requires each client to create their own
developer app for security reasons. So there is no possibility for one user to access data of another user.

To create a Criteo developer app and get a client id and secret, follow the following steps:

 - Create a developer account [here](https://developers.criteo.com/)
 - In the section "My apps", create an application
 - Name the application, eg. Keboola Criteo App
 - In the "Service" section select "Marketing solutions" and press save
 - In the "Authorization" set all access to read access and press save
 - On the top right of the app creation page press the "Activate App" button
 - Press create key. A text file containing the client id and secret should be downloaded
 - Again on the top right of the app creation page click the "Generate new URL" button
 - Send this consent link to your Criteo account administrator, who then has to accept the access (Only an administrator can accept the consent screen)

Now you can use the client id and secret obtained from the text file


## Configuration
 - Client ID (#client_id) - [REQ] Client ID from Criteo developer APP
 - Client secret (#client_secret) - [REQ] Client secret from Criteo developer APP

## Row Configuration
 - Output name (out_table_name) - [REQ] Name of output table in storage eg. CampaignReport
 - Dimensions (dimensions) - [REQ] Dimensions to fetch for report eg. CampaignId, Day, Category
 - Metrics (metrics) - [REQ] Metrics to fetch for report eg. Clicks, Displays, AdvertiserCost
 - Currency (currency) - [OPT] Currency for report, default EUR
 - Date range type (date_range) - [REQ] Custom, Last week (sun-sat), Last month
 - Date from (date_from) - [OPT] Start date of the report eg. 3 days ago
 - Date to (date_to) - [OPT] End date of the report eg. 1 day ago
- Loading options (loading_options) - [REQ] What type of loading; Full overwrites the existing table in storage and incremental appends new and updates existing passes in the table using a primary key.
  - Incremental - [OPT] true/false
  - Primary Key - [OPT] Necessary for incremental load type
