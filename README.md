# CiviCRM Contact Export

A Node script for exporting all contact data from CiviCRM to CSV and JSON files.

## What is this?

I needed to get all my contact data out of CiviCRM. I can't run the usual CiviCRM 'Export all contacts' because reasons, but I can run a copy of the MySQL database on my local machine. So, here's a script that gets that data out nicely.

The script dumps all basic contact data (email, address, phone) plus all custom fields into a CSV file and JSON file.

It's very rough and ready, and made for a specific use case, but it should be a good starting point if you have similar needs.

## Installation

You will need Node.js installed. Clone the repo then install modules with `npm install`

## Usage

```sh
$ node run
```

## Config and defaults

Defaults are loaded from `defaults.yml`. If you want to override any of these keys, create a file in the project root called `config.yml` and use any of the same keys, e.g.

```yaml
dbport: 3306
drupaldbname: my_drupal_db_name      # [String] Name of your Drupal database
civicrmdbname: my_civi_db_name       # [String] Name of your CiviCRM database
```

## Important — workaround for useless code

I need to unserialize some of my custom contact data and split it into different CSV files. This code is left in because I haven't got around to refactoring it out yet. It's probably a good example for you if you need to unserialize anything, otherwise you can just get rid of most of it. The block has a comment at the top of it with instructions. 

Basically, replace this:

```js
    .then(function(result){
        console.log(statusMsg('Unserializing dashboard data for '+result.length+' rows...'));
        // more useless unserialization logic here
        return {
            csv : {
                contacts: contacts,
                donations: donations,
                recurringDonations: recurringDonations,
                reportedIncome: reportedIncome
            },
            json: result
        }
    })
```

with:

```js
    .then(function(result){
        return {
            csv : {
                contacts: result,
            },
            json: result
        }
    })
```

