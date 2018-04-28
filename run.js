// PACKAGES

// Database
console.log('Booting...')
var mysql = require('promise-mysql')
var Promise = require('bluebird')
var PromiseQueue = require('bluebird-queue')
console.log('Loaded MySQL and Promise libs')

// filesystem
var fs = Promise.promisifyAll(require('fs'))
var path = require('path')
var rimraf = require('rimraf')
var mkdirp = require('mkdirp')
// var rsync = require('rsyncwrapper').rsync
console.log('Loaded filesystem libs')
// CSV
var json2csv = require('json2csv')
var toCSV = Promise.promisify(json2csv)
console.log('Loaded CSV lib')
// utilities
var moment = require('moment')
var trim = require('trim')
var yamljs = require('yamljs')
var clone = require('clone')
// var unique = require('array-unique')
var ProgressBar = require('progress')
var merge = require('merge')
var chalk = require('chalk')
var md5 = require('md5')
// var Levenshtein = require('levenshtein')

console.log('Loaded utility libs')

// make the console pretty
var alertMsg = chalk.cyan
var successMsg = chalk.green
var errorMsg = chalk.bold.red
var warningMsg = chalk.bold.yellow
var statusMsg = chalk.dim.gray

// load arguments from defauts.yml if it exists
var defaults = yamljs.load('./defaults.yml')

// load local arguments from config.yml if it exists
try {
  var config = yamljs.load('./config.yml')
  merge(defaults, config)
} catch (e) {
  config = false
}

// / DB Connection details

var drupalDB = defaults['drupaldbname']
var civicrmDB = defaults['civicrmdbname']
var dbhost = defaults['dbhost']
var dbport = defaults['dbport']
var dbuser = defaults['dbuser']
var dbpassword = defaults['dbpassword']
var dataDir = defaults['datadirectory']

// //////////////////////////////////////////////////
// /////////////  END CONFIGURATION /////////////////
// //////////////////////////////////////////////////

// global connection vars
var drupal
var civicrm

// //////////////////////////////////////////////////
// /////////////  MAIN CONTROLLER ///////////////////
// //////////////////////////////////////////////////

var run = function () {
  console.log(chalk.bold.red.bgWhite(' Scraping the database! '))
  console.log(statusMsg('Scrape started at ' + moment().format()))

  return new Promise(function (resolve) {
    // start the promise chain so that all subsequent calls are nicely nested in .then() blocks
    resolve()
  })
  .then(function (conn) {
    // Uncomment this to add a connection to your Drupal DB
    //      // try to connect to the Drupal database
    //      return connectToDB(drupalDB)
    // })
    // .then(function(conn){
    //      drupal = conn;
    //      console.log(statusMsg('connected to Drupal database...'));
    return connectToDB(civicrmDB)
  })
  .then(function (conn) {
    civicrm = conn
    console.log(statusMsg('connected to CiviCRM database...'))
  })
  .then(function () {
    console.log(statusMsg('Clearing data directory...'))
    // create data directory if it doesn't exist
    // clear it out if it does
    return new Promise(function (resolve, reject) {
      rimraf(path.join(__dirname, dataDir), function (err) {
        if (err) {
          return reject(err)
        }
        mkdirp(path.join(__dirname, dataDir), function (err) {
          if (err) {
            return reject(err)
          }
          return resolve()
        })
      })
    })
  })
  .then(function () {
    return execute()
  })
  .then(function (data) {
    if (data) {
      console.log(successMsg('Returned ' + data.json.length + ' contacts'))

      // // queue CSVs for writing
      var queue = new PromiseQueue()
      Object.keys(data.csv).forEach(function (key) {
        if (data.csv[key].length > 0) {
          console.log(statusMsg('Queueing ' + key + ' for CSV conversion'))
          queue.add(
            toCSV({data: data.csv[key]})
            .then(function (csv) {
              csv = csv.replace(/"null"/g, '""')
              var filename = key + '.csv'
              return fs.writeFileAsync(path.join(__dirname, dataDir, filename), csv)
              .then(function () {
                console.log(alertMsg('Wrote file ' + filename))
              })
            })
          )
        }
      })
      // queue .json file for writing
      var jsonFile = 'data.json'
      queue.add(
        fs.writeFileAsync(path.join(__dirname, dataDir, jsonFile), JSON.stringify(data.json, null, '\t'))
        .then(function () {
          console.log(alertMsg('Wrote file ' + jsonFile))
        })
      )
      return queue
    } else {
      console.log(warningMsg('No rows returned...'))
    }
  })
  .then(function (queue) {
    queue.start().then(function (result) {
      console.log(successMsg('All data saved!'))
    })
  })
  .catch(function (error) {
    console.trace(errorMsg(error))
  })
  .finally(function () {
    civicrm.destroy()
    // drupal.destroy();        // uncomment if you opened a Drupal database connection above
  })
}

// //////////////////////////////////////////////////
// /////////////  SCRIPT METHODS ////////////////////
// //////////////////////////////////////////////////

function connectToDB (dbname) {
  return mysql.createConnection({
    host: dbhost,
    port: dbport,
    user: dbuser,
    password: dbpassword,
    database: dbname
  })
}

function exit () {
  throw new Error('Exit requested by script')
}

function execute () {
  var contacts = {}

  return new Promise(function get_custom_tables (resolve) {
    // empty promise to kick things off
    // easier to run each SQL query in it's own .then() block
    console.log(alertMsg('Running SQL...'))
    resolve()
  })
  .then(function () {
    // select all custom groups
    var q = []
    q.push('SELECT ' + fields('custom_group') + ' FROM `civicrm_custom_group` custom_group')
    return query(q)
  })
  .then(function (result) {
    // get a list of the tables containing custom field data
    var queue = new PromiseQueue()
    var custom_tables = result
    var custom_table_schema = {}
    result.forEach(function (row) {
      var q = []
      q.push('SELECT column_name FROM information_schema.columns WHERE table_name="' + row.table_name + '"')
      queue.add(query(q))
    })
    return queue.start().then(function (results) {
      for (var i = 0, j = results.length; i < j; i++) {
        custom_table_schema[custom_tables[i].table_name] = []
        results[i].forEach(function (result) {
          custom_table_schema[custom_tables[i].table_name].push(result.column_name)
        })
      }
      return custom_table_schema
    })
  })
  .then(function build_query (custom_table_schema) {
    // select basic contact details
    var q = []
    q.push('SELECT ' + fields(['contact', 'email', 'address', 'phone']))
    // q.push(', GROUP_CONCAT(groups.title SEPARATOR \';\') as groups')
    q.push(',' + custom_table_fields(custom_table_schema, false, 'full') + ' ')
    q.push('FROM civicrm_contact contact')
    // contact details
    q.push('LEFT OUTER JOIN (SELECT ' + fields('email') + ' FROM `civicrm_email` email WHERE email.is_primary=1) email ON contact.id=email.contact_id')
    q.push('LEFT OUTER JOIN (SELECT ' + fields('address') + ' FROM `civicrm_address` address WHERE address.is_primary=1) address ON contact.id=address.contact_id')
    q.push('LEFT OUTER JOIN (SELECT ' + fields('phone') + ' FROM `civicrm_phone` phone WHERE phone.is_primary=1) phone ON contact.id=phone.contact_id')
    // groups
    // q.push('LEFT OUTER JOIN (SELECT contact_id,title FROM `civicrm_group_contact` JOIN `civicrm_group` ON civicrm_group_contact.group_id = civicrm_group.id) groups ON contact.id=groups.contact_id')
    // add data from custom fields
    Object.keys(custom_table_schema).forEach(function (custom_table) {
      var name = custom_table_name(custom_table)
      q.push('LEFT OUTER JOIN (SELECT ' + custom_table_fields(custom_table_schema, custom_table, 'as') + ' FROM ' + custom_table + ' ' + name + ') ' + name)
      q.push('ON contact.id=' + name + '.' + name + '__entity_id')
    })
    // restrict selection
    q.push('WHERE (')
    q.push('contact.contact_type="Individual"')
    q.push('AND NOT contact.is_deleted')
    q.push('AND (membershipstatus.membershipstatus__giving_what_we_can_member="1" OR membershipstatus.membershipstatus__trying_out_giving="1" OR membershipstatus.membershipstatus__my_giving_user="1")')
    q.push(')')
    // q.push('AND contact.id=375')
    return query(q)
  })
  // // This .then() block is very specific to my particular setup, but might be a useful
  // // example if you're trying to process some serialized data that's being stored in Civi.
  // // If you want to just dump the results, replace the contents of this block with the following:
  // //
  // //        return {
  // //            csv : {
  // //                contacts: result,
  // //            },
  // //            json: result
  // //        }
  // //
  .then(function process_results (result) {
    console.log(statusMsg('Unserializing dashboard data for ' + result.length + ' rows...'))
    // console.log(result[Math.floor(Math.random()*result.length)])
    var contacts = []
    var donations = []
    var reportedIncome = []
    var recurringDonations = []
    var charities = []
    var charityMergeCandidates = []
    var unknownCharities = []
    var donationCurrencyCodes = []
    var donationHashes = []

    // var contactGroups = [];
    var bar = new ProgressBar('Unserialising: [:bar]', { total: result.length })
    // split dashboard out from main contact
    for (var i = 0, j = result.length; i < j; i++) {
      bar.tick()
      var contact = result[i]
      contact.donations = null
      contact.recurringDonations = null
      contact.reportedIncome = null;
      [
        'birth_date',
        'dates__joining_date',
        'dates__left_date_former_members_',
        'dates__end_date_try_out_givers_',
        'dates__start_date_trying_giving_',
        'outreach__date_started',
        'outreach__date_finished',
        'outreach__next_contact_date',
        'legacydata__date_old_pledge_form_submitted'
      ].forEach(function (field) {
        var m = moment(contact[field])
        if (m.isValid()) {
          contact[field] = m.toISOString()
        } else {
          contact[field] = null
        }
      })
      // dashboard
      var dashboardData = contact.donations__dashboard_data
      if (dashboardData && dashboardData !== 'No data') {
        try {
          dashboardData = JSON.parse(trim(dashboardData))
        } catch (err) {
          console.log(errorMsg('Error: this dashboard data could not be parsed:'))
          console.log(statusMsg(dashboardData))
          throw (err)
        }
        // add back singleton values that should stay with the contact
        contact.donations__defaultcurrency = dashboardData.defaultcurrency
        contact.donations__yearstartdate = dashboardData.yearstartdate
        contact.donations__yearstartmonth = dashboardData.yearstartmonth
        contact.donations__lastupdated = dashboardData.lastupdated
        contact.donations__public = dashboardData.public
        // split dashboard into donations, and identify by contact ID
        if (dashboardData.donations && dashboardData.donations.length > 0) {
          dashboardData.donations.forEach(function (donation) {
            var charityName = trim(donation[1])
            // get charity name for master list
            if (charities.indexOf(charityName) === -1) {
              charities.push(charityName)
            }
            // get currency codes
            var donationCurrencyCode = donation[2]
            if (donationCurrencyCodes.indexOf(donationCurrencyCode) === -1) donationCurrencyCodes.push(donationCurrencyCode)
            // create a deterministic unique identifier for each donation
            var donationHash = contact.id + '-' + md5(JSON.stringify(donation))
            if (donationHashes.indexOf(donationHash) === -1) {
              donationHashes.push(donationHash)
            } else {
              throw new Error(donationHash + ' is not a unique hash')
            }
            var d = {
              donation_md5_hash: donationHash,
              donation_contact_id: contact.id,
              donation_timestamp: moment(+donation[0] * 1000).toISOString(),
              donation_target: charityName,
              donation_currency: donationCurrencyCode,
              donation_amount: donation[3]
            }
            donations.push(d)
            contact.donations = contact.donations || []
            contact.donations.push(d)
          })
        }
        // split dashboard into income, and identify by contact ID
        if (dashboardData.income) {
          Object.keys(dashboardData.income).forEach(function (year) {
            // dates
            var m = moment([year, dashboardData.yearstartmonth + 1, dashboardData.yearstartdate].join('-'), 'YYYY-M-D')
            var startDate = m.toISOString()
            var endDate = m.add(1, 'year').subtract(1, 'day').toISOString()

            // pledge percentage
            var pledgePercentage = contact.pledgedamounts__pledge_percentage
            if (!pledgePercentage) {
              if (contact.membershipstatus__giving_what_we_can_member === 1) {
                pledgePercentage = 10
              } else {
                pledgePercentage = 0
              }
            }

            var income = dashboardData.income[year]
            var ri = {
              income_contact_id: contact.id,
              income_start_date: startDate,
              income_end_date: endDate,
              income_currency: income[0],
              income_amount: income[1],
              income_pledge_percentage: pledgePercentage
            }
            reportedIncome.push(ri)
            contact.reportedIncome = contact.reportedIncome || []
            contact.reportedIncome.push(ri)
          })
        }
        // split out recurring donations, and identify by contact ID
        if (dashboardData.recurringdonations && dashboardData.recurringdonations.length > 0) {
          dashboardData.recurringdonations.forEach(function (donation) {
            var charityName = trim(donation[4])
            if (charities.indexOf(charityName) === -1) {
              charities.push(charityName)
            }
            var rd = {
              recurring_donation_contact_id: contact.id,
              recurring_donation_start_timestamp: moment(+donation[0] * 1000).toISOString(),
              recurring_donation_end_timestamp: donation[1],
              recurring_donation_frequency_unit: donation[2],
              recurring_donation_frequency: donation[3],
              recurring_donation_target: charityName,
              recurring_donation_currency: donation[5],
              recurring_donation_amount: donation[6]
            }
            recurringDonations.push(rd)
            contact.recurringDonations = contact.recurringDonations || []
            contact.recurringDonations.push(rd)
          })
        }
      }

      /* // contact groups
      var importantGroups = [
        "Members",
        "My Giving signups",
        "Trying Giving"
      ]
      var obsoleteGroups = [
        "1000 members party",
        "Alison Woodman",
        "AMF donor information email",
        "Giving What We Can Oxford",
        "Hidden Smart Group 16",
        "Members who haven't logged in yet",
        "Members without join dates",
        "Members without pledge estimates",
        "Non-members' newsletter",
        "People contacted through outreach",
        "Signups with real donations",
        "Test Group (Jacob, Michelle and Steph)",
        "Trying Giving people who haven't logged in yet",
        "UK Members",
        "Unconfirmed Members",
        "Unconfirmed Trying Giving",
        "US members",
        "US Members email no.2",
        "US members email no.3",
        "Users citing 80,000 Hours",
        "Wrong birth date",
        "Wrong birth date (new CRM)",
        "z members from old CRM",
        "z try givers from old CRM",
        "z try givers from old CRM who we forgot about"
      ]

      if(contact.groups){
        contact.groups = contact.groups.split(';');
        var contactImportantGroups = [];
        var contactObsoleteGroups = [];
        [importantGroups,obsoleteGroups].forEach(function(groupType,index){
          groupType.forEach(function(groupTitle){

          })
        })
        console.log()

        contact.groups.forEach(function(groupTitle){
          console.log('title',groupTitle)
          contactGroups.push({
            'contact_group_contact_id' : contact.id,
            'contact_group_group_title' : groupTitle
          })
        })
      } */
      // get rid of the dashboard data from the main array, including if it's null
      delete contact.donations__dashboard_data
      // get rid of donations from the CSV data but not the JSON data
      contact = clone(contact)
      delete contact.donations
      delete contact.recurringDonations
      delete contact.reportedIncome
      // delete contact.groups;
      contacts.push(contact)
    }
    if (unknownCharities.length > 0) {
      console.log(unknownCharities.length, 'Unknown Charities:')
      console.log(unknownCharities)
    }
    /* charities.sort();
    console.log(statusMsg('Checking for similar charity names...'))
    bar = new ProgressBar('Processing: [:bar]',{ total: charities.length });
    var minDistance = 4;
    charities.forEach(function(charityA,indexA){
      charities.forEach(function(charityB,indexB){
        if(indexA !== indexB && charityA.length>minDistance && charityB.length>minDistance && indexB>indexA){
          var comparison = new Levenshtein(charityA,charityB)
          if(comparison.distance<minDistance) charityMergeCandidates.push({
            charityA: charityA,
            charityB: charityB
          })
        }
      })
      bar.tick();
    }) */
    donationCurrencyCodes.sort()
    donationCurrencyCodes = donationCurrencyCodes.map(function (currency_code) {
      return {
        'currency_code': currency_code
      }
    })
    charities = charities.map(function (charity) {
      return {
        'charity_name': charity,
        'charity_type': 'Unknown'
      }
    })
    return {
      csv: {
        contacts: contacts,
        donations: donations,
        recurringDonations: recurringDonations,
        reportedIncome: reportedIncome,
        charities: charities,
        charityMergeCandidates: charityMergeCandidates,
        donationCurrencyCodes: donationCurrencyCodes
        // contactGroups: contactGroups
      },
      json: result
    }
  })
}

function query (queryStringArray, db) {
  db = db || 'civicrm'
  var queryString = queryStringArray.join('\n') + ';'
  console.log(alertMsg('SQL QUERY:'))
  console.log(statusMsg(queryString))
  if (db === 'civicrm') {
    return civicrm.query(queryString)
  } else if (db === 'drupal') {
    return drupal.query(queryString)
  }
}

function fields (table, tableName) {
  tableName = tableName || table
  var fields_names = {
    contact: [
      'id',
      'source',
      'first_name',
      'last_name',
      'prefix_id',
      'suffix_id',
      'job_title',
      'gender_id',
      'birth_date'
    ],
    email: [
      'contact_id',
      'email'
    ],
    address: [
      'contact_id',
      'street_address',
      'supplemental_address_1',
      'supplemental_address_2',
      'supplemental_address_3',
      'city',
      'postal_code'
    ],
    phone: [
      'contact_id',
      'phone'
    ],
    group_contact: [
      'contact_id',
      'group_id'
    ],
    group: [
      'id',
      'title'
    ],
    custom_group: [
      'id',
      'name',
      'table_name'
    ]
  }

  output = []
  if (Array.isArray(table)) {
    table.forEach(function (t) {
      fields_names[t].forEach(function (field) {
        if (field !== 'contact_id') {
          output.push(t + '.' + field)
        }
      })
    })
  } else {
    fields_names[table].forEach(function (field) {
      output.push(tableName + '.' + field)
    })
  }
  return output.join(',')
}

function custom_table_fields (schema, custom_table, namespacing) {
  namespacing = namespacing || false
  var output = []

  if (custom_table) {
    var name = custom_table_name(custom_table)
    schema[custom_table].forEach(function (column) {
      if (column !== 'id') {
        output.push(name + '.' + column)
      }
    })
  } else {
    Object.keys(schema).forEach(function (table) {
      var name = custom_table_name(table)
      schema[table].forEach(function (column) {
        if (column !== 'id') {
          output.push(name + '.' + column)
        }
      })
    })
  }

  if (namespacing) {
    if (namespacing.toLowerCase() === 'as') {
      for (var i = 0, j = output.length; i < j; i++) {
        output[i] = output[i] + ' AS ' + custom_table_column_namespace(output[i])
      }
    } else if (namespacing.toLowerCase() === 'full') {
      for (var i = 0, j = output.length; i < j; i++) {
        var t = output[i].split('.')[0]
        output[i] = t + '.' + custom_table_column_namespace(output[i])
      }
    }
  }

  return output.join(',')
}

function custom_table_column_namespace (input) {
  // select column names with nice namespacing
  var x = input.split('.')
  var table = x[0]
  var column = x[1]
  column = column.split('_')
  if (!isNaN(column[column.length - 1])) { column.pop() }
  column = column.join('_')
  return table + '__' + column
}

function custom_table_name (name) {
  var name = name.replace('civicrm_value_', '')
  name = name.split('_')
  name.pop()
  return name.join('')
}

run()
