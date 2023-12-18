const express = require('express')
const {Client} = require('pg')
const multer = require('multer')
const {v4: uuidv4} = require('uuid');
require('dotenv').config()

const app = express()
const {DB_USER, DB_PASSWORD, DB_HOST, DB_NAME, DB_PORT} = process.env

const config = {
    user: DB_USER,
    password: DB_PASSWORD,
    host: DB_HOST,
    database: DB_NAME,
    port: Number(DB_PORT)
}

const client = new Client(config);

app.use(express.json())

const upload = multer({storage: multer.memoryStorage()})

app.post('/uploads', upload.single('test'), async (req, res, next) => {
    try {

        const {originalname, buffer, size} = req.file;
        const fileId = uuidv4()

        const query = {
            text: 'INSERT INTO mm_notice_file (file_id, file_name, file_size, file_content) VALUES ($1, $2, $3, $4)',
            values: [fileId, originalname, size, buffer]
        }

        await client.query(query)

        res.status(201).json({fileId})
    } catch (err) {
        next(err);
    }
})

app.get('/downloads', async (req, res, next) => {
    try {
        const {fileId} = req.query

        const query = {
            text: 'SELECT * FROM mm_notice_file WHERE file_id=$1',
            values: [fileId]
        }
        const result = await client.query(query)

        if (!result.rows.length) {
            throw new Error('empty data!')
        }

        const {file_content, file_name} = result.rows[0]
        res.set('Content-disposition', 'attachment; filename=' + file_name);
        res.set('Content-Type', 'text/plain')

        res.status(200).end(file_content)
    } catch (err) {
        console.error(err);
        res.status(400).json(err)
    }
})

app.listen(4000, () => {
    console.log('Server Start!')

    client.connect().then(() => {
        console.log('Postgres Connect!')
    }).catch(console.error)
})