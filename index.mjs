import fetch from 'node-fetch';
import pg from 'pg';
import AdmZip from 'adm-zip';
import iconv from 'iconv-lite';
import {pipeline} from 'node:stream';
import {promisify} from 'node:util'
import path from 'path';
import fs from 'fs';
import xmljs from 'xml-js';

const streamPipeline = promisify(pipeline);

const ZIP_LINK = 'http://www.cbr.ru/s/newbik';
const TEMP_PATH = path.join(process.cwd(), 'temp');
try {
    fs.mkdirSync(TEMP_PATH);
} catch (error) {
    // console.error(error);
}
const ZIP_PATH = path.join(TEMP_PATH, 'SBR.zip');
const EXTRACT_ZIP_PATH = path.join(TEMP_PATH, 'SBR');
const MAX_DEBUG = process.argv.includes('--debug') ? 3 : 0;

// download zip
const cbrWriteZipStream = fs.createWriteStream(ZIP_PATH);
const cbrResponse = await fetch(ZIP_LINK);
await streamPipeline(cbrResponse.body, cbrWriteZipStream);

// extract zip to folder
const zip = new AdmZip(ZIP_PATH);
zip.extractAllTo(EXTRACT_ZIP_PATH, true); // true - override

console.log('EXTRACT_ZIP_PATH', EXTRACT_ZIP_PATH);
const xmlRegExp = /\.xml$/i;
const list = fs.readdirSync(EXTRACT_ZIP_PATH).filter(n => xmlRegExp.test(n));
// console.log('list', list);

const result = [];

for (let i = 0; i < list.length; i++) {
    const fileName = list[i];
    // console.log('fileName', fileName);
    const xmlString = iconv.decode(fs.readFileSync(path.join(EXTRACT_ZIP_PATH, fileName)), 'win1251');
    // console.log('xmlString', xmlString.length, xmlString.slice(0, 500));
    const data = xmljs.xml2js(xmlString, {compact: true, ignoreComment: true});
    // console.log('1 level', Object.keys(data));
    // console.log('   _declaration', Object.keys(data['_declaration']));
    // console.log('       _attributes', data['_declaration']['_attributes']);
    // console.log('   ED807', Object.keys(data['ED807']));
    // console.log('       _attributes', data['ED807']['_attributes']);
    for (let j = 0; j < (MAX_DEBUG || data['ED807']['BICDirectoryEntry'].length); j++) {
        const bicRecord = data['ED807']['BICDirectoryEntry'][j];
        if (MAX_DEBUG) {
            console.log(`${j}:  `, bicRecord);
        }
        if (!bicRecord['Accounts']) {
            continue;
        }
        for (let acci = 0; acci < bicRecord['Accounts'].length; acci++) {
            const accountRecord = bicRecord['Accounts'][acci];
            result.push({
                bic: bicRecord._attributes.BIC,
                name: bicRecord.ParticipantInfo._attributes.NameP,
                corrAccount: accountRecord._attributes.Account
            })
        }
    }
}
const nRand = Math.floor(Math.random() * result.length - 2 ) + 1;
console.log('result: ', result[nRand - 1], result[nRand], result[nRand + 1]);

if (process.argv.includes('--pg')) {

const PG_CONFIG = {
    user: 'postgres',
    password: 'admin',
    host: '127.0.0.1',
    database: 'postgres',
    port: 5432
}
const TABLE = 'BIC_accounts';

const client = new pg.Client(PG_CONFIG)
await client.connect();
console.log('pg connected')

// const t = await client.query('SELECT NOW()');
// console.log('SELECT NOW()', t.rows);

await client.query(`
    CREATE TABLE IF NOT EXISTS ${TABLE} (
        bic VARCHAR(200) NOT NULL,
        name VARCHAR(200) NOT NULL,
        corrAccount VARCHAR(200) NOT NULL
    )
`);

let updated = 0;
let created = 0;

for (let i = 0; i < (MAX_DEBUG || result.length); i++) {
    const bicRecord = result[i];
    const isExists = await client.query(`
        SELECT * FROM ${TABLE}
        WHERE corrAccount = '${bicRecord.corrAccount}'
    `);
    if (isExists.rows.length) {
        await client.query(`
            UPDATE ${TABLE} 
            SET bic = '${bicRecord.bic}', name = '${bicRecord.name}', corrAccount = '${bicRecord.corrAccount}'
            WHERE corrAccount = '${bicRecord.corrAccount}'
        `);
        updated++;
    } else {
        await client.query(`
            INSERT INTO ${TABLE} 
            (bic, name, corrAccount) 
            VALUES(
                '${bicRecord.bic}',
                '${bicRecord.name}',
                '${bicRecord.corrAccount}'
            )
        `);
        created++;
    }
}
if (MAX_DEBUG) {
    console.log(`updated: ${updated}, created: ${created}`)
}

await client.end();

}
