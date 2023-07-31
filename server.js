const express = require('express');
const app = express();
const { spawn } = require('child_process');
const { exec } = require('child_process');
// const sleep = require('util').promisify(setTimeout);
const multer = require('multer');
const upload = multer();
const { v4: uuidv4 } = require('uuid');

const fse = require('fs-extra');
const csv = require('csv-parser');

const fs = require('fs');
const path = require('path');
const cron = require('node-cron');
const schedule = require('node-schedule');
const readline = require('readline');
const axios = require('axios');
const json2csv = require('json2csv').parse;
const util = require('util');
const AWS = require('aws-sdk');
require('dotenv').config();
const nodemailer = require('nodemailer');
const createCsvWriter = require('csv-writer').createObjectCsvWriter;


const MAX_RETRIES = 3;
const TIMEOUT = 5000; // in ms



const readFile = util.promisify(fs.readFile);
const writeFile = util.promisify(fs.writeFile);


const sgMail = require("@sendgrid/mail");
const sgKey = process.env.SG_KEY;
sgMail.setApiKey(sgKey);
console.log(`sgKey = ${sgKey}`)
const { Configuration, OpenAIApi } = require("openai");



const PULL_MIN = 5;
const PULL_HOUR = 1;
const MAX_PULLS_PER_DAY = 2000;
const REQS_PER_BATCH = 400;





function replaceSpecialChars(str) {
  var specialChars = ['#', '?', '&', '=', '/', '%', '+', ':',' '];
  var newStr = str;

  specialChars.forEach(function(char) {
      var regex = new RegExp('\\' + char, 'g');
      newStr = newStr.replace(regex, '-');
  });

  return newStr;
}







async function trimColumns(fileName) {
  let results = [];
  let headers = [];
  
  return new Promise((resolve, reject) => {
      fs.createReadStream(fileName)
          .pipe(csv())
          .on('headers', (h) => {
              headers = h.slice(0, h.indexOf('SeoDescription')+1);
          })
          .on('data', (row) => {
              let newRow = {};
              headers.forEach((header) => {
                  newRow[header] = row[header];
              });
              results.push(newRow);
          })
          .on('end', async () => {
              const tempFileName = `${path.parse(fileName).name}-temp.csv`;
              const csvWriter = createCsvWriter({
                  path: tempFileName,
                  header: headers.map((header) => ({id: header, title: header})),
              });
              
              await csvWriter.writeRecords(results);
              
              // Replace original file with the new one
              fs.unlink(fileName, err => {
                  if (err) {
                      reject(err);
                  } else {
                      fs.rename(tempFileName, fileName, err => {
                          if (err) {
                              reject(err);
                          } else {
                              resolve('The CSV file was written successfully');
                          }
                      });
                  }
              });
          });
  });
}

async function getCompanyName(company, maxRetries = 3, delay = 1000) {
  const configuration = new Configuration({
    apiKey: process.env.OPENAI_KEY,
  });
  const openai = new OpenAIApi(configuration);

  for (let i = 0; i < maxRetries; i++) {
    try {
      const chatCompletion = await openai.createChatCompletion({
        model: "gpt-3.5-turbo",
        messages: [{
          role: "user", 
          content: `Give me a formatted company name for ${company}, no legal abbreviations or descriptors of the service, just the unique part of name, normal casing no all uppercase.`
        }],
        temperature: 0.0
      });
      return chatCompletion.data.choices[0].message.content;
    } catch (error) {
      if (error.response && error.response.status === 503 && i < maxRetries - 1) {
        console.log(`Request failed with status code 503. Retrying after ${delay}ms...`);
        await sleep(delay);
      } else {
        throw error;
      }
    }
  }
}

const ID = process.env.AZ_ID;
const SECRET = process.env.AZ_SECRET;

const BUCKET_NAME = "apollo-pulls";

const s3 = new AWS.S3({
    accessKeyId: ID,
    secretAccessKey: SECRET
});



async function appendJobToJsonFile(path, newJsonObj) {
  console.log("TODO: append job to json file");
}

async function runMissedJobs() {
  console.log("TODO: run missed jobs and knock the ones that have either been ran already or ");
}


async function uploadAndClearFile(filePath) {
  console.log(filePath);
  const fileExists = fs.existsSync(filePath);
  if (!fileExists) {
      console.log(`File does not exist: ${filePath}`);
      return;
  }
  await correctHeaders(filePath);
  await uploadFile(filePath);
  fs.unlinkSync(filePath);
}

async function addNewKey(BUCKET_NAME,filename,key,value){
  let fileJson = await downloadJSONFromS3(BUCKET_NAME,filename);
  console.log(`fileJson: ${JSON.stringify(fileJson, null, 2)}`);
  fileJson[key] = value;
  uploadJsonToS3(BUCKET_NAME, filename, fileJson);

}

async function getOrganizationData(orgId,key){
  try {
    const response = await axios.get(`https://api.apollo.io/v1/organizations/${orgId}`, {
      headers: {
        'Content-Type': 'application/json',
        'Cache-Control': 'no-cache'
      },
      params: {
        api_key: key
      }
    });

    // console.log(response.data);
    return response.data.organization;
  } catch (error) {
    console.error(error);
  }
};


function downloadFromS3(fileName, res) {
  const params = {
    Bucket: BUCKET_NAME,
    Key: fileName
  };

  // Create a read stream from the S3 object
  const s3Stream = s3.getObject(params).createReadStream();

  // Set the headers to force file download
  res.setHeader('Content-Disposition', `attachment; filename=${path.basename(fileName)}`);
  res.setHeader('Content-Type', 'text/csv');

  // Pipe the s3 stream to the response
  s3Stream.pipe(res).on('error', (err) => {
    console.error(`Error streaming file from S3: ${err}`);
    res.status(500).send('Error streaming file from S3');
  }).on('finish', () => {
    console.log(`File streamed from S3: ${fileName}`);
  });
}

async function downloadJSONFromS3(bucketName,fileName) {
  const params = {
    Bucket: bucketName,
    Key: fileName,
  };

  try {
    // Fetch the object from S3
    const data = await s3.getObject(params).promise();

    // Convert the Body to a string and then parse it into a JSON object
    const jsonData = JSON.parse(data.Body.toString());

    // Log the result for debug
    console.log(`File downloaded from S3: ${fileName}`);
    
    // Return the JSON data
    return jsonData;
  } catch (err) {
    console.error(`Error downloading file from S3: ${err}`);
    throw err;
  }
}



function uploadFile(fileName){
  // Read content from the file
  const fileContent = fs.readFileSync(fileName);

  // Setting up S3 upload parameters
  const params = {
      Bucket: BUCKET_NAME,
      Key: path.basename(fileName), // File name you want to save as in S3
      Body: fileContent
  };

  // Uploading files to the bucket
  s3.upload(params, function(err, data) {
      if (err) {
          throw err;
      }
      console.log(`File uploaded successfully. ${data.Location}`);
  });
};


async function changeCsvHeaders(file, oldHeader, newHeader) {
  try {
      let data = await readFile(file, 'utf8');

      // Convert oldHeader to quoted string
      oldHeader = `\"${oldHeader}\"`;

      // Split the file into lines
      let lines = data.split('\n');

      // Modify the line you're interested in
      let headers = lines[0].split(',');

      for (let i = 0; i < headers.length; i++) {
          if (headers[i] == oldHeader){
              console.log(`Found ${oldHeader} at index ${i}`)
              headers[i] = newHeader;
          }
      }

      // Update the headers here as per your requirements
      lines[0] = headers.join(',');

      // Join the lines back into a single string
      let output = lines.join('\n');

      // Write the file back out
      await writeFile(file, output, 'utf8');
  } catch (err) {
      console.log(err);
  }
}


async function uploadJsonToS3(bucketName, key, jsonData) {
  // Convert JSON object to a string
  const jsonString = JSON.stringify(jsonData);

  // Set up S3 upload parameters
  const params = {
      Bucket: bucketName,
      Key: key, // The file name you want to save as in S3
      Body: jsonString,
      ContentType: "application/json"
  };

  try {
      // Upload the JSON string to S3
      const data = await s3.upload(params).promise();
      console.log(`File uploaded successfully. ${data.Location}`);
  } catch (err) {
      console.error(`Error uploading file to S3: ${err}`);
      throw err;
  }
}



async function correctHeaders(file){
  console.log("CHECKING HEADERS");
  await changeCsvHeaders(file, "linkedin_url", "Person Linkedin Url");
  await changeCsvHeaders(file, "first_name", "First Name");
  await changeCsvHeaders(file, "last_name", "Last Name");
  await changeCsvHeaders(file, "name", "Full Name");
  await changeCsvHeaders(file, "title", "Title");
  await changeCsvHeaders(file, "twitter_url", "Twitter Url");
  await changeCsvHeaders(file, "github_url", "Github Url");
  await changeCsvHeaders(file, "facebook_url", "Facebook Url");
  await changeCsvHeaders(file, "organization_Name", "Company Name");
  await changeCsvHeaders(file, "country", "Country");
}


function readJsonFile(path) {
  return new Promise((resolve, reject) => {
      fs.readFile(path, 'utf8', (err, data) => {
          if (err) {
              reject(err);
          } else {
              resolve(JSON.parse(data));
          }
      });
  });
}

function appendToJsonFile(path, newJsonObj) {
  // Read existing data
  fs.readFile(path, 'utf8', (err, data) => {
      if (err){
          console.log(`Error reading file from disk: ${err}`);
      } else {
         // Parse the JSON string to object
         const fileData = JSON.parse(data);

         // Add your new JSON object
         fileData.push(newJsonObj);

         // Write new data back to the file
         fs.writeFile(path, JSON.stringify(fileData, null, 2), (err) => {
             if (err) console.log(`Error writing file: ${err}`);
         });
      }
  });
}

function getValueFromKey(filename, key) {
  return new Promise((resolve, reject) => {
      fs.readFile(filename, 'utf-8', (err, data) => {
          if (err) {
              return reject(err);
          }

          let obj = JSON.parse(data);

          if(key in obj) {
              resolve(obj[key]);
          } else {
              resolve(null);
          }
      });
  });
}


function checkIfKeyExists(filePath, key) {
  fs.readFile(filePath, 'utf-8', (err, data) => {
      if (err) {
          console.log('Error reading file:', err);
          return;
      }

      // Parse the file content to a JavaScript object
      let obj = JSON.parse(data);

      // Check if key exists
      if(key in obj) {
          console.log('The key exists!');
          return true;
      } else {
          console.log('The key does not exist.');
          return false;
      }
  });
}


function appendKeyToFile(filename, key, value, callback) {
  fs.readFile(filename, 'utf-8', (err, data) => {
      if (err) {
          return callback(err);
      }

      // Parse the file content to a JavaScript object
      let obj = JSON.parse(data);

      // Append new key
      obj[key] = value;

      // Write the updated object back to the file
      fs.writeFile(filename, JSON.stringify(obj, null, 2), err => {
          if (err) {
              return callback(err);
          }
          callback(null, obj);
      });
  });
}


function getValueFromCSV(filename, key) {
  // Read the contents of the CSV file
  const data = fs.readFileSync(filename, 'utf8');

  // Split the data into an array of rows
  const rows = data.trim().split('\n');

  // Loop through each row
  for (let i = 0; i < rows.length; i++) {
    const columns = rows[i].split(',');

    // Check if the key matches the first column of the current row
    if (columns[0] === key) {
      // Return the value in the second column
      return Number(columns[1]); // Assuming the value in the second column is a number
    }
  }

  // If the key was not found, return 0
  return 1;
}

function updateCSVFile(filename, key, value) {
  // Check if file exists
  if (fs.existsSync(filename)) {
    // Read file content and split into rows
    const fileContent = fs.readFileSync(filename, 'utf-8');
    const rows = fileContent.trim().split('\n').map(row => row.split(','));

    // Find row with matching key and update value
    const rowIndex = rows.findIndex(row => row[0] === key);
    if (rowIndex !== -1) {
      rows[rowIndex][1] = value;
    } else {
      // Add new row with key and value
      rows.push([key, value]);
    }

    // Write updated rows to file
    const csvString = rows.map(row => row.join(',')).join('\n');
    fs.writeFileSync(filename, csvString);
  } else {
    // File does not exist, create new file with header and data
    const header = 'Key,Value\n';
    const data = `${key},${value}\n`;
    const csvString = header + data;
    fs.writeFileSync(filename, csvString);
  }
}


async function readFileAsListOfLists(filePath) {
  let fileStream = fs.createReadStream(filePath);

  const rl = readline.createInterface({
      input: fileStream,
      crlfDelay: Infinity
  });

  let listOfLists = [];
  
  for await (const line of rl) {
      // Split the line into a list and add it to listOfLists
      listOfLists.push(line.split(":"));
  }

  return listOfLists;
}




async function req(endpoint, proxy, reqData, page, retries = MAX_RETRIES) {
  reqData.page = page;

  try {
    let res = await axios.get(endpoint, {
      params: reqData,
      proxy: {
        protocol: 'http',
        host: proxy[0],
        port: proxy[1],
        auth: {
          username: proxy[2], 
          password: proxy[3]  
        }
      },
      timeout: TIMEOUT
    });

    return res.data;
  } catch (error) {
    if (error.response && error.response.status === 429) {
      console.error('Rate limit exceeded. Stopping further requests...');
      return null; // Stop making requests
    } else if (retries) { // Retry on error or timeout
      console.log(`Request to ${endpoint} failed, retrying...`);
      return await req(endpoint, proxy, reqData, page, retries - 1);
    } else {
      console.error(`Error making request to ${endpoint}: ${error}`);
      throw error;
    }
  }
}


async function testCredentials(api_key, prxFile) {
  let proxyList = await readFileAsListOfLists(prxFile);  // the function argument prxFile is used here

  let currProx = proxyList[Math.floor(Math.random() * proxyList.length)];  // corrected line
  console.log(`Using proxy ${currProx[0]}:${currProx[1]} with username ${currProx[2]} and password ${currProx[3]}`);
  const url = "https://api.apollo.io/v1/contacts/search";

  const data = {
    api_key: api_key,
    q_keywords: "Tim Zheng, CEO, Apollo",
    sort_by_field: "contact_last_activity_date",
    sort_ascending: false
  };

  const headers = {
    "Content-Type": "application/json",
    "Cache-Control": "no-cache"
  };

  try {
    const response = await axios.post(url, data, { 
      headers, 
      proxy: {
        protocol: 'http',
        host: currProx[0],
        port: currProx[1],
        auth: {
            username: currProx[2], 
            password: currProx[3]  
        }
      }
    });
    console.log(response.status);
    return response.status;
  } catch (error) {
    console.log("Creds Not Valid");
    return error.response.status;
  }
}





function apolloRequestData(url, api_key) {
  // console.log(`${url} is url`);
  const decodedUrl = decodeURIComponent(url);
  url = decodedUrl.replace(/%5B/g, '[').replace(/%5D/g, ']');
  let params = url.split("?")[1].split("&");


  // console.log(params)
  let reqData = {};
  reqData.api_key = api_key;
  revRangeArgs = {};
  for (let i of params){
      if (i.includes("[]")){
          let paramName = i.split("[]")[0].replace(/([A-Z])/g, "_$1").toLowerCase();
          let paramArg = i.split("=")[1].replace(/%20/g," ");
          if (reqData.hasOwnProperty(paramName)){
              reqData[paramName].push(paramArg);
              // reqData[paramName] = [paramArg];

          }else{
              reqData[paramName] = [paramArg];
          }
      }else if (i.includes("[max]") || i.includes("[min]")){
          if (i.includes("[min]")){
              revRangeArgs.min = i.split("=")[1].replace(/%20/g," ");
          }else{
              revRangeArgs.max = i.split("=")[1].replace(/%20/g," ");
          }
          reqData.revenue_range = revRangeArgs;
      }
      
  }
  console.log(`REQ DATA: ${JSON.stringify(reqData, null, 2)}`);
  return reqData;
  
}


let transporter = nodemailer.createTransport({
  host: "smtp.gmail.com", // replace with your email provider's SMTP server
  port: 587,
  secure: false, // true for 465, false for other ports
  auth: {
      user: process.env.WORKER_ID, // replace with your email address
      pass: process.env.WORKER_PASS  // replace with your email password
  }
});




const sendEmail = async (text, subject, to) => {
  const msg = {
      to: to, // Change to your recipient
      from: 'worker@icepick.io', // Change to your verified sender
      subject: subject,
      text: text,
      // html: html,
  };

  try {
      await sgMail.send(msg);
      console.log('Email sent');
  } catch (error) {
      console.error(error);
      if (error.response) {
          console.error(error.response.body)
      }
  }
};




function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}



async function trialPull(requestData) {
  await getLeads(requestData.url, requestData.api_key, 1000, requestData.email,requestData.searchID);
  
  let fileName = requestData.searchID + ".csv";
  let subject = "Enjoy your free trial leads!";
  let body = `Hi there,\n\nThanks for trying out our service. Here are your leads brev!\n\nhttp://ApolloPullsAutoScaler-1-1638492219.us-east-2.elb.amazonaws.com/download-page?fileName=${fileName}`;
  let sender = "worker@icepick.io";
  let recipient = requestData.email;
  
  sendEmail(body, subject, recipient);
  console.log(fileName);
  uploadAndClearFile(`./output/${fileName}`);
}


function printReadableDate(date) {
  const options = {
    weekday: 'long',
    year: 'numeric',
    month: 'long',
    day: 'numeric',
    hour: 'numeric',
    minute: 'numeric',
    second: 'numeric',
    timeZone: 'UTC'
  };
  const formattedDate = date.toLocaleString('en-US', options);
  console.log(formattedDate);
}

function scheduleFile(jobScheduleObj,requestData) {
  schedulerObj = {};
  let job_day = jobScheduleObj.now;
  // job_day.setMinutes((job_day.getMinutes() + 1) % 60);
  console.log(`pls work ${job_day}`);

  console.log(jobScheduleObj);
  let jobObjects = []; // Array to store job objects
  fileJobObjs = [];
  subject = "Your leads are processing!";
  body = `Hi there,\n\nThanks for trying out our service. Your leads are currently processing - we'll send you an email when they're finished`;
  sender = "worker@icepick.io";
  console.log(requestData.email);
  recipient = requestData.email;
  sendEmail(body,subject,recipient);
  
  dayLoop: for (let i = 0; i < jobScheduleObj.days; i=i+1) {
    

      
    for(let j = 0; j < jobScheduleObj.times.length; j=j+1){
      
      job_day.setHours(jobScheduleObj.times[j].hour);
      job_day.setMinutes((jobScheduleObj.times[j].minute + (5*i)) % 60);
      let fileObj = {};
      fileObj.job_time = job_day;
      fileObj.url =  requestData.url;
      fileObj.api_key = requestData.api_key;
      fileObj.email = requestData.email;
      fileObj.searchID = requestData.searchID;
      fileObj.type = "pull";

      // console.log(`reqsLeft = ${jobScheduleObj.reqsLeft} and REQS_PER_BATCH = ${REQS_PER_BATCH}}`)
      // console.log(`running job at ${job_day}`);
      if (jobScheduleObj.reqsLeft > (REQS_PER_BATCH)){
        fileObj.numLeads =  REQS_PER_BATCH;
        
        
        const job = schedule.scheduleJob(`job_${jobScheduleObj.currJob}`, job_day, () => getLeads(requestData.url, requestData.api_key, REQS_PER_BATCH * 10, requestData.email,requestData.searchID));

        jobObjects.push({jobName: `job_${jobScheduleObj.currJob}`, job}); // Adding the job to the array
        console.log(`Pulling ${REQS_PER_BATCH * 10} leads at ${job_day}`)
        
      }else{
        

        let currReqsLeft = jobScheduleObj.reqsLeft;
        fileObj.numLeads = currReqsLeft*10;
        const job = schedule.scheduleJob(`job_${jobScheduleObj.currJob}`, job_day, () => getLeadsFinal(requestData.url, requestData.api_key, currReqsLeft*10, requestData.email,requestData.searchID));
        jobObjects.push({jobName: `job_${jobScheduleObj.currJob}`, job}); // Adding the job to the array
        console.log(`Pulling ${currReqsLeft * 10} leads at ${job_day}`)

        break dayLoop;



        
      //   console.log(`Full Leads email scheduled for ${job_day}`)

      }
      fileJobObjs.push(fileObj);
      if (jobScheduleObj.currJob == 0){
        let finalDate = new Date(job_day);
        finalDate.setMinutes(jobScheduleObj.times[jobScheduleObj.batches % jobScheduleObj.times.length].hour);
        finalDate.setMinutes(jobScheduleObj.times[jobScheduleObj.batches % jobScheduleObj.times.length].minute + 30);

        
        fileObj.job_time = job_day;
        fileObj.body = body;
        fileObj.subject = subject;
        fileObj.sender = sender;
        fileObj.recipient = recipient;

        
        fileObj.type = "email";
        fileJobObjs.push({jobName: `finalEmail`, fileObj});
        // console.log("TODO: send email to user that leads are processing with html that leads them to icepick.io include the time they'll finish")
      }
      
      
      
      // printReadableDate(job_day);
      jobScheduleObj.currJob++;
      jobScheduleObj.reqsLeft -= (REQS_PER_BATCH);

      
    }
    job_day.setDate(job_day.getDate() + 1);
  

    
  }
  
  let data = JSON.stringify(fileJobObjs, null, 2);
  appendToJsonFile("schedule.json",fileJobObjs);


  return jobObjects; // Return the array of job objects
  
}



function runScript(reqData, numLeads) {
  const command = `node processing_files/pull.cjs ${reqData.url} ${reqData.api_key} ${numLeads} ${reqData.email} random ${reqData.searchID}`;

  exec(command, (error, stdout, stderr) => {
    if (error) {
      console.error(`Error executing Python script: ${error}`);
      return;
    }

    // Output from the Python script
    console.log(stdout);

    // Error messages (if any)
    console.error(stderr);
  });
}

function getDayOfYear() {
  const now = new Date();
  const start = new Date(now.getFullYear(), 0, 0);
  const diff = now - start;
  const oneDay = 1000 * 60 * 60 * 24;
  const day = Math.floor(diff / oneDay);
  return day;
}



function jobScheduleObj(numLeads){
  scheduler = {};
  scheduler.currJob = 0;
  let requests = numLeads/10;
  let now = new Date();
  now.setMinutes(now.getMinutes() + 1);
  
  let hours = now.getHours();
  let minutes = now.getMinutes();
  // console.log(`Current time is: ${hours}:${minutes}`);
  const days = Math.ceil(requests / MAX_PULLS_PER_DAY);
  const currentDay = getDayOfYear();
  // console.log(currentDay);
  let finalDay = currentDay + days;
  let batches = Math.ceil(requests / (REQS_PER_BATCH/10))
  let lastDayBatches = batches % MAX_PULLS_PER_DAY;
  if (lastDayBatches == 0){
    lastDayBatches = MAX_PULLS_PER_DAY;
  }
  scheduler.days = days;
  scheduler.batches = batches;
  scheduler.lastDayBatches = lastDayBatches;
  scheduler.times = [];
  scheduler.reqsLeft = requests; // Correct
  scheduler.now = now;


  console.log(`DAYS:${days} | BATCHES:${batches} | LAST_DAY: ${lastDayBatches}`);
  for (let i = 0; i < (MAX_PULLS_PER_DAY / REQS_PER_BATCH);i++){
    let batchHour = (hours);
    let batchMinute = (minutes);
    hours = (PULL_HOUR + hours) % 24;
    minutes = (PULL_MIN + minutes) % 60;
    if ((minutes == 0) && (i != 0)) hours = (hours + 1) % 24;
    let batchTime = {};
    batchTime.hour = batchHour;
    batchTime.minute = batchMinute;
    

    
    
    scheduler.times.push(batchTime)
    
    

  }
  return scheduler;


}


function generateUniqueID() {
  const timestamp = new Date().getTime();
  return timestamp.toString();
}

function sendText(subject,text) {
  

  const mailOptions = {
    from: process.env.WORKER_ID, // sender address
    to: process.env.DEV_NUM, // list of receivers
    subject: subject, // Subject line
    body: text// Plain text body
  };

  transporter.sendMail(mailOptions, function (err, info) {
    if(err)
      console.log(err)
    else
      console.log(info);
  });
}

app.get('/', (req, res) => {
  res.send('Hello, world!');
  // console.log("Req made to serv")
});






app.post('/process',upload.none(),async (req, res) => {
  let { url,numLeads, api_key,email,server_key,searchID } = req.body;
  console.log(`URL: ${url} | NUM-LEADS:${numLeads} | API-KEY:${api_key} | EMAIL:${email} | SERVKEY:${server_key}`);
  numLeads = Math.ceil(numLeads / 10) * 10;
  searchID = replaceSpecialChars(searchID);
  console.log(`search id ${searchID}`);

  
  let invoicePaid;
  // sendText("New Request",`User ${email} running a search for ${numLeads} Leads`)
  sendEmail(`User ${email} running a search for ${numLeads} Leads | URL = ${url} | searchID = ${searchID}`,"NEW REQUEST ON LEADPULL","andrew@icepick.io");
  // sendEmail(`User ${email} running a search for ${numLeads} Leads`,"NEW REQUEST ON LEADPULL","ryeem@icepick.io");

  
  try {
    invoicePaid = await downloadJSONFromS3(BUCKET_NAME,"usedKeys.json");
    invoicePaid = invoicePaid[api_key];
    console.log(`invoicePaid: ${invoicePaid}`)
    
  } catch (err) {
    console.error('Failed to get value:', err);
  }
  if (invoicePaid == null){
    invoicePaid = false;
    await addNewKey(BUCKET_NAME,"usedKeys.json",api_key,false);
    console.log("hi");
    await addNewKey(BUCKET_NAME,"emails.json",email,api_key);
  }else if (invoicePaid == false){
    res.status(401).send({text: `YOU MUST PAY THE TOLL BROKIE`});
    return;
  }

  let validCreds = await testCredentials(api_key,"proxies.txt")
  
  if (validCreds == 401 ){
    res.status(401).send({text: `Invalid Credentials`});
    return;
  }else if (validCreds > 250){
    res.status(401).send({text: `No clue what happened`}); 
    return;
  }
  

  

  
  let reqID = uuidv4();
  let reqObj = {};
  reqObj.url = url;
  reqObj.numLeads = numLeads;
  reqObj.api_key = api_key;
  reqObj.email = email;
  reqObj.server_key = server_key;
  reqObj.searchID = reqID+"-"+searchID;
  // console.log(`numLeads: ${numLeads} | invoicePaid: ${invoicePaid}`)
  
  console.log(reqObj.searchId);
  
  if (invoicePaid == true){
    let schedulerObj = jobScheduleObj(numLeads);
    scheduleFile(schedulerObj,reqObj);
    res.status(200).send({text: `Processing request with ID: ${reqID} | Filename: ${reqObj.searchID}.csv`});
  }else{
    trialPull(reqObj);
    res.status(200).send({text: `Yuo only get 1k lest u pay up brev`});
  }

 
});






app.get('/download', async (req, res) => {
  let fileName = req.query.fileName;

  console.log(`Received request to download file: ${fileName}`);
  console.log(`Received request with query: ${JSON.stringify(req.query)}`);
  
  let localFilePath = `./output/${fileName}`;
  // uploadAndClearFile(localFilePath);
  await correctHeaders(localFilePath);
  
  if (fs.existsSync(localFilePath)) {  // check if file exists without adding the directory
    console.log(`File found locally: ${fileName}`);
    res.download(localFilePath, path.basename(localFilePath), (err) => {  // use path.basename to get the file name only
      if (err) {
        console.error(`Error downloading file: ${err}`);
        res.status(500).send('Error downloading file');
      } else {
        console.log(`File downloaded: ${localFilePath}`);
      }
    });
  } else {
    console.log(`File not found locally: ${fileName}`);
    // Stream from S3
    downloadFromS3(fileName, res);
  }
});









app.get('/download-page', (req, res) => {
  let fileName = req.query.fileName;
  res.send(`
      <html>
          <body>
              <script>
                  window.onload = function() {
                      window.location.href = "/download?fileName=${fileName}";
                  }
              </script>
          </body>
      </html>
  `);
});



async function getLeads(url,api_key, numLeads,email,searchID){
  console.log(`Pulling ${numLeads} leads from ${url}`);
  const startingPage = getValueFromCSV('processing_files/personas.csv',searchID);
  console.log(`Starting page: ${startingPage}`);
  // console.log(searchID);
  let proxyList = await readFileAsListOfLists("proxies.txt");
  // console.log(proxyList);
  let firstRun = (startingPage == 1);
  let pages = Math.ceil(numLeads/10) + startingPage - firstRun;
  let currProx = Math.floor(Math.random() * proxyList.length);
  

  console.log(pages)
  
  for (let i = startingPage + !firstRun; i <= pages; i++) {
      console.log(`Pulling page ${i} of ${pages}`)
      if(i % 100 == 0 && i != 0){
          currProx = Math.floor(Math.random() * proxyList.length);
          console.log(`Switching proxies to ${proxyList[currProx][0]} | ${i} pages pulled`)
      }
      let data = await req('https://api.apollo.io/v1/mixed_people/search?',proxyList[currProx],apolloRequestData(url,api_key,i),i);
      // console.log(`people: ${data.people}`);
      if (Object.keys(data.people).length === 0) {
        console.log(`No more leads`);
        let fileName = searchID + ".csv";
        let subject = "Enjoy your free trial leads!";
        let body = `Hi there,\n\nThanks for trying out our service. Here are your leads brev!\n\nhttp://ApolloPullsAutoScaler-1-1638492219.us-east-2.elb.amazonaws.com/download-page?fileName=${fileName}`;
        let sender = "worker@icepick.io";
        let recipient = email;
        
        sendEmail(body, subject, recipient);
        return;
      }
      if (data == null) {
        console.log(`Too many reqs`);
        return;
      }
      // console.log(data);
      // console.log(data.people);
      await sleep(3000);
      const fields = [
        
        // 'industry',
        'first_name',
        'last_name',
        'name',
        'linkedin_url',
        'title',
        'email_status',
        'photo_url',
        'twitter_url',
        'github_url',
        'facebook_url',
        'extrapolated_email_confidence',
        'headline',
        'email',
        'organization_id',
        // 'employment_history',
        'State',
        'City',
        'country',
        // 'organization',
        // 'phone_numbers',
        'organization_Name',
        'Website',
        'companyLinkedin',
        'id',
        'CompanyState',
        'CompanyCity',
        'SeoDescription',
        // 'formattedCompany',
        "-"
        


        // 'intent_strength',
        // 'show_intent',
        // 'revealed_for_current_team'
      ];  
      // console.log(data.people[0].last_name);
      for (let i = 0; i < data.people.length; i++) {
        data.people[i]["-"] = "";
        
        // await sleep(250);
        if (data.people[i].organization) {

            data.people[i].organization_Name = data.people[i].organization.name ;
            data.people[i].Website = data.people[i].organization.website_url ;
            data.people[i].companyLinkedin = data.people[i].organization.linkedin_url ;
            let orgData = await getOrganizationData(data.people[i].organization.id,api_key);
            data.people[i].CompanyState = orgData.state ;
            data.people[i].CompanyCity = orgData.city ;
            data.people[i].SeoDescription = orgData.seo_description ;
            

        } else {
            data.people[i].organization_Name;
            data.people[i].Website;
            data.people[i].companyLinkedin;
        }
        
        data.people[i].State = data.people[i].state ;
        data.people[i].City = data.people[i].city ;
      }

      
      
    
      // const includeHeaders = (i===1);

      const opts = { fields };

      
      try {
        let csv = json2csv(data.people, opts);
        

        fs.appendFileSync(`output/${searchID}.csv`, csv);
        console.log('File saved successfully!');
      } catch (err) {
          console.error(err);
      }
      updateCSVFile('processing_files/personas.csv', searchID,i);

  }
  await trimColumns(`./output/${searchID}.csv`);

}




async function getLeadsFinal(url,api_key, numLeads,email,searchID){
  await getLeads(url,api_key, numLeads,email,searchID)
  let fileName = searchID + ".csv";
  subject = "Your Leads are Ready!";
  body = `Hi there,\n\nThanks for trying out our service. Here are your leads brev!\n\nhttp://ApolloPullsAutoScaler-1-1638492219.us-east-2.elb.amazonaws.com/download-page?fileName=${fileName}`;
  // await deleteColumnsWithNoHeaders(`./output/${searchID}.csv`,`./output/${searchID}_temp.csv`)
  await trimColumns(`./output/${searchID}.csv`);
  await correctHeaders(`./output/${searchID}.csv`);
  sendEmail(body,subject,email);
  uploadAndClearFile(`./output/${searchID}.csv`);

}

process.on('uncaughtException', function(err) {
  console.log('Caught exception: ', err);
  console.log('Stack trace: ', err.stack);  // This line added
  // sendText(`ERROR ON SERV`,`Caught exception: ${err}, Stack: ${err.stack}`);
  sendEmail(`Caught exception: ${err}, Stack: ${err.stack}`,"SERVER ERROR","andrew@icepick.io");
});




const port = 3000;
app.listen(port, () => {
  console.log(`Server running on port ${port}`);
});

async function main() {
  console.log("Starting server...");
  const jobData = await readJsonFile('schedule.json');
  // console.log(schedule);
  let jobObjects = [];

  jobData.forEach((job) => {
    // console.log(job);
    let timeString = job.job_time;
    let time = new Date(timeString);
    if (time > new Date()){
      console.log(`Job ${job.type}  is scheduled and will run at the scheduled ${job.job_time}.`);
      const Newjob = schedule.scheduleJob(`job_${jobScheduleObj.currJob}`, job.job_time, () => getLeads(job.url, job.api_key, job.numLeads, job.email,job.searchID));

      jobObjects.push({jobName: `job_${jobScheduleObj.currJob}`, job}); // Adding the job to the array
      // const jobObj = schedule.scheduleJob(job.jobName, time, () => runScript(job, job.numLeads));
    }
    
  });

}


main();