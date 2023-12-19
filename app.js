const express = require('express')
const {Client} = require('pg')
const multer = require('multer')
const {v4: uuidv4} = require('uuid');
require('dotenv').config()

const {DB_USER, DB_PASSWORD, DB_HOST, DB_NAME, DB_PORT} = process.env

const config = {
    user: DB_USER,
    password: DB_PASSWORD,
    host: DB_HOST,
    database: DB_NAME,
    port: Number(DB_PORT)
}
const client = new Client(config);

const app = express()
app.use(express.json())

// 파일을 디스크에 저장하지 않고 메모리에 임시저장
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

        // 찾으려는 데이터가 없으면 에러
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
        const {table_schema} = req.query
        const insertQuery = {
            text: 'INSERT INTO column_sync (id, table_name, table_schema, column_name, data_type)\n' +
                'SELECT \n' +
                '  cur.table_schema || \'_\' || cur.column_name,\n' +
                '  cur.table_name, \n' +
                '  cur.table_schema, \n' +
                '  cur.column_name, \n' +
                '  cur.data_type\n' +
                'FROM \n' +
                '  information_schema.columns AS cur\n' +
                'FULL JOIN \n' +
                '  column_sync AS sync\n' +
                'ON \n' +
                '  cur.table_schema || \'_\' || cur.column_name = sync.id\n' +
                'WHERE \n' +
                '  cur.table_schema = $1\n' +
                '  AND sync.id IS NULL',
            values: [table_schema]
        }
        const insertResult = await client.query(insertQuery)
        console.log('insertResult: ', insertResult.rowCount)

        const updateQuery = {
            text: 'UPDATE column_sync AS sync\n' +
                'SET \n' +
                '  id = cur.table_schema || \'_\' || cur.column_name,\n' +
                '  data_type = cur.data_type\n' +
                'FROM \n' +
                '  information_schema.columns AS cur\n' +
                'WHERE \n' +
                '  cur.table_schema = $1\n' +
                '  AND sync.id = cur.table_schema || \'_\' || cur.column_name\n' +
                '  AND sync.data_type <> cur.data_type',
            values: [table_schema]
        }
        const updateResult = await client.query(updateQuery)
        console.log('updateResult: ', updateResult.rowCount)

        const deleteQuery = {
            text: 'DELETE FROM column_sync AS sync\n' +
                'WHERE \n' +
                '  sync.id IN (\n' +
                '    SELECT cur.table_schema || \'_\' || cur.column_name\n' +
                '    FROM information_schema.columns AS cur\n' +
                '    WHERE cur.table_schema = $1\n' +
                '  )\n' +
                '  AND NOT EXISTS (\n' +
                '    SELECT 1 \n' +
                '    FROM information_schema.columns AS cur\n' +
                '    WHERE sync.id = cur.table_schema || \'_\' || cur.column_name\n' +
                '  )',
            values: [table_schema]
        }
        const deleteResult = await client.query(deleteQuery)
        console.log('deleteResult: ', deleteResult.rowCount)

        // const result = await client.query('WITH merged_data AS (\n' +
        //     '    SELECT \n' +
        //     '        COALESCE(sync.id, cur.table_schema || \'_\' || cur.column_name) AS sync_id,\n' +
        //     '        cur.table_name,\n' +
        //     '        cur.table_schema,\n' +
        //     '        cur.column_name,\n' +
        //     '        cur.data_type AS new_data_type,\n' +
        //     '        sync.data_type AS existing_data_type\n' +
        //     '    FROM information_schema.columns AS cur\n' +
        //     '    LEFT JOIN column_sync AS sync\n' +
        //     '    ON sync.table_schema = cur.table_schema \n' +
        //     '    AND sync.id = cur.table_schema || \'_\' || cur.column_name\n' +
        //     ')\n' +
        //     'INSERT INTO column_sync (id, table_name, table_schema, column_name, data_type)\n' +
        //     'SELECT \n' +
        //     '    sync_id,\n' +
        //     '    table_name, \n' +
        //     '    table_schema, \n' +
        //     '    column_name, \n' +
        //     '    new_data_type\n' +
        //     'FROM merged_data\n' +
        //     'WHERE sync_id IS NOT NULL\n' +
        //     '\tON CONFLICT (id) DO UPDATE\n' +
        //     '\tSET \n' +
        //     '\t    id = EXCLUDED.id,\n' +
        //     '\t    data_type = EXCLUDED.data_type\n' +
        //     '\tWHERE column_sync.id = EXCLUDED.id\n' +
        //     '\tAND column_sync.data_type <> EXCLUDED.data_type;')

        console.log(result.rowCount)
        res.status(200).end()
    } catch (err) {
        console.error(err);
        res.status(400).json(err)
    }
})

// app.get('/refresh', async (req, res, next) => {
//     try {
//         const tempHash = {}
//
//         const syncArray = await client.query('SELECT * FROM column_sync').then(rows)
//         for (const row of syncArray) {
//             tempHash[row.id] = row
//         }
//
//         const query = {
//             text: 'SELECT table_name, table_schema, column_name, data_type\n'
//                 + 'FROM information_schema.columns\n'
//                 + 'WHERE table_schema=$1',
//             values: ['qbig']
//         }
//         const currentDataList = await client.query(query).then(rows)
//         for (const row of currentDataList) {
//             const {table_name, table_schema, column_name, data_type} = row
//             const id = `${table_schema}_${column_name}`
//             // hash에 있으면
//             if (tempHash[id]) {
//                 // 데이터 타입이 다르면
//                 if (tempHash[id].data_type !== data_type) {
//                     // update query
//                     console.log('update!')
//                     const updateQuery = {
//                         text: 'UPDATE column_sync SET data_type=$1 WHERE id=$2',
//                         values: [data_type, id]
//                     }
//                     await client.query(updateQuery)
//                 }
//             } else {
//                 // 없으면 insert query
//                 console.log('insert!')
//                 const insertQuery = {
//                     text: 'INSERT INTO column_sync (id, table_name, table_schema, column_name, data_type) VALUES ($1, $2, $3, $4, $5)',
//                     values: [id, table_name, table_schema, column_name, data_type]
//                 }
//                 await client.query(insertQuery)
//             }
//
//             delete tempHash[id]
//         }
//
//         // tempHash에 남아있는 데이터 삭제처리
//         for (const key in tempHash) {
//             console.log('delete!')
//             const deleteQuery = {
//                 text: 'DELETE FROM column_sync WHERE id=$1',
//                 values: [key]
//             }
//             await client.query(deleteQuery)
//         }
//
//         res.status(200).end()
//     } catch (err) {
//         console.error(err);
//         res.status(400).json(err)
//     }
// })

app.listen(4000, () => {
    console.log('Server Start!')

    client.connect().then(() => {
        console.log('Postgres Connect!')
    }).catch(console.error)
})