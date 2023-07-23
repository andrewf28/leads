const AWS = require('aws-sdk');
const fs = require('fs');
const path = require('path');
const util = require('util');
const readFile = util.promisify(fs.readFile);
const writeFile = util.promisify(fs.writeFile);

const ID = process.env.AZ_ID;
const SECRET = process.env.AZ_SECRET;

console.log(ID);
console.log(SECRET);

const BUCKET_NAME = "apollo-pulls";

const OUTPUT_PATH = "./output/";


const s3 = new AWS.S3({
    accessKeyId: ID,
    secretAccessKey: SECRET
});



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
  
  
  
  
  
  async function correctHeaders(file){
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
  
  


async function uploadFile(fileName){
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
  


async function uploadAndClear(dirPath) {
    const files = fs.readdirSync(dirPath);
    await Promise.all(files.map(async (file) => {
        console.log(file);
        const filePath = path.join(dirPath, file);
        await correctHeaders(filePath);
        await uploadFile(filePath);
        fs.unlinkSync(filePath);
    }));
}

uploadAndClear(OUTPUT_PATH);