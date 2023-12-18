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

const rows = (result) => result.rows

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

app.get('/refresh', async (req, res, next) => {
    try {
        const tempHash = {}

        const syncArray = await client.query('SELECT * FROM column_sync').then(rows)
        for (const row of syncArray) {
            tempHash[row.id] = row
        }

        const query = {
            text: 'SELECT table_name, table_schema, column_name, data_type\n'
                + 'FROM information_schema.columns\n'
                + 'WHERE table_schema=$1',
            values: ['qbig']
        }
        const currentDataList = await client.query(query).then(rows)
        for (const row of currentDataList) {
            const {table_name, table_schema, column_name, data_type} = row
            const id = `${table_schema}_${column_name}`
            // hash에 있으면
            if (tempHash[id]) {
                // 데이터 타입이 다르면
                if (tempHash[id].data_type !== row.data_type) {
                    // update query
                    console.log('update!')
                    const updateQuery = {
                        text: 'UPDATE column_sync SET data_type=$1 WHERE id=$2',
                        values: [row.data_type, id]
                    }
                    await client.query(updateQuery)
                }
                delete tempHash[id]
            } else {
                // 없으면 insert query
                console.log('insert!')
                const insertQuery = {
                    text: 'INSERT INTO column_sync (id, table_name, table_schema, column_name, data_type) VALUES ($1, $2, $3, $4, $5)',
                    values: [id, table_name, table_schema, column_name, data_type]
                }

                await client.query(insertQuery)
                delete tempHash[row.id]
            }
        }

        // tempHash에 남아있는 데이터 삭제처리
        for (const key in tempHash) {
            console.log('delete!')
            const deleteQuery = {
                text: 'DELETE FROM column_sync WHERE id=$1',
                values: [key]
            }
            await client.query(deleteQuery)
        }

        res.status(200).end()
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