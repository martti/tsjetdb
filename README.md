# MS Access JETDB3 & JETDB4 library for Node.js Apps

## Installation

### Install `tsjetdb`

```sh
$ npm install tsjetdb --save
```

## Usage

### Opening the database

```typescript
import JetDb from 'tsjetdb'

const db = new JetDb('file.mdb')

for (const r of db.rows('TABLE')) {
  console.log(r.columns[0].value)
}

db.streamRows('TABLE').on('data', rows => {
  for (const r of rows) {
    console.log(r.columns[0].value)
  }
})
```
