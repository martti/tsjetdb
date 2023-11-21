import { describe, expect, it, beforeAll } from '@jest/globals'
import { JetDb, Row } from '../src/jetdb'

describe('JetDb tests', () => {
  it('should throw unknown database version', () => {
    const jetdb = new JetDb('./tests/data/testV2000.mdb')
    expect(() => {
      jetdb['databaseConfig'](
        Buffer.from([
          0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2,
        ]),
      )
    }).toThrow('Unknown database version 2')
  })

  it('should throw unknown database version for Access 2020 files', () => {
    expect(() => new JetDb('./tests/data/testV2010.accdb')).toThrow(
      'Unknown database version 3',
    )
  })
})

describe('JetDb 4 tests', () => {
  let jetdb: JetDb
  beforeAll(() => {
    jetdb = new JetDb('./tests/data/testV2000.mdb')
  })

  it('should have 4 tables', () => {
    expect(jetdb.tables().sort()).toEqual(
      ['Table1', 'Table2', 'Table3', 'Table4'].sort(),
    )
  })

  it('table 1 should have 9 columns', () => {
    expect(jetdb.columns('Table1').length).toEqual(9)
  })

  it('table 2 should have 89 columns', () => {
    expect(jetdb.columns('Table2').length).toEqual(89)
  })

  it('table 1 columns should be abcdefghi', () => {
    expect(jetdb.columns('Table1').sort()).toEqual([
      'A',
      'B',
      'C',
      'D',
      'E',
      'F',
      'G',
      'H',
      'I',
    ])
  })

  it('table 1 have 2 rows', () => {
    expect(jetdb.rows('Table1').length).toEqual(2)
  })

  it('table 1 columns should have correct values', () => {
    const rows = jetdb.rows('Table1')
    expect(rows[0].columns[0].value).toEqual('abcdefg')
    expect(rows[0].columns[1].value).toEqual('hijklmnop')
    expect(rows[0].columns[2].value).toEqual(2)
    expect(rows[0].columns[3].value).toEqual(222)
    expect(rows[0].columns[4].value).toEqual(333333333)
    expect(rows[0].columns[5].value).toEqual(444.555)
    expect(rows[0].columns[6].value).toEqual(4673231456670056448n) // should be 09/21/74 00:00:00
    expect(rows[0].columns[7].value).toEqual('[unknown type]') // should be money 3.5000
    expect(rows[0].columns[8].value).toEqual(1)
    expect(rows[1].columns[0].value).toEqual('a')
    expect(rows[1].columns[1].value).toEqual('b')
    expect(rows[1].columns[2].value).toEqual(0)
    expect(rows[1].columns[3].value).toEqual(0)
    expect(rows[1].columns[4].value).toEqual(0)
    expect(rows[1].columns[5].value).toEqual(0)
    expect(rows[1].columns[6].value).toEqual(4673956859466481664n) // 12/12/81 00:00:00
    expect(rows[1].columns[7].value).toEqual('[unknown type]') // money 0.0000
    expect(rows[1].columns[8].value).toEqual(0)
  })

  it('table 2 should have no rows', () => {
    const rows = jetdb.rows('Table2')
    expect(rows.length).toEqual(0)
  })
})

describe('JetDb 4 tests 2', () => {
  let jetdb: JetDb
  beforeAll(() => {
    jetdb = new JetDb('./tests/data/test2V2000.mdb')
  })

  it('should have 1 table', () => {
    expect(jetdb.tables().sort()).toEqual(['MSP_PROJECTS'].sort())
  })

  it('table 1 should have 74 columns', () => {
    expect(jetdb.columns('MSP_PROJECTS').length).toEqual(74)
  })

  it('table 1 have 1 row', () => {
    expect(jetdb.rows('MSP_PROJECTS').length).toEqual(1)
  })

  it('table 1 columns should have correct values', () => {
    const rows = jetdb.rows('MSP_PROJECTS')
    expect(rows[0].columns[0].value).toEqual(1)
    expect(rows[0].columns[1].value).toEqual('Project1')
    // expect(rows[0].columns[2].value).toEqual('[unknown type]')
    expect(rows[0].columns[2].value).toEqual(
      'Jon Iles this is a a vawesrasoih aksdkl fas dlkjflkasjd flkjaslkdjflkajlksj dfl lkasjdf lkjaskldfj lkas dlk lkjsjdfkl; aslkdf lkasjkldjf lka skldf lka sdkjfl;kasjd falksjdfljaslkdjf laskjdfk jalskjd flkj aslkdjflkjkjasljdflkjas jf;lkasjd fjkas dasdf asd fasdf asdf asdmhf lksaiyudfoi jasodfj902384jsdf9 aw90se fisajldkfj lkasj dlkfslkd jflksjadf as',
    )
  })
})

describe('JetDb 3 tests', () => {
  let jetdb: JetDb
  beforeAll(() => {
    jetdb = new JetDb('./tests/data/testV1997.mdb')
  })

  it('should have 4 tables', () => {
    expect(jetdb.tables().sort()).toEqual(
      ['Table1', 'Table2', 'Table3', 'Table4'].sort(),
    )
  })

  it('table 1 should have 9 columns', () => {
    expect(jetdb.columns('Table1').length).toEqual(9)
  })

  it('table 2 should have 89 columns', () => {
    expect(jetdb.columns('Table2').length).toEqual(89)
  })

  it('table 1 columns should be abcdefghi', () => {
    expect(jetdb.columns('Table1').sort()).toEqual([
      'A',
      'B',
      'C',
      'D',
      'E',
      'F',
      'G',
      'H',
      'I',
    ])
  })

  it('table 1 have 2 rows', () => {
    expect(jetdb.rows('Table1').length).toEqual(2)
  })

  it('table 1 columns should have correct values', () => {
    const rows = jetdb.rows('Table1')
    expect(rows[1].columns[0].value).toEqual('abcdefg')
    expect(rows[1].columns[1].value).toEqual('hijklmnop')
    expect(rows[1].columns[2].value).toEqual(2)
    expect(rows[1].columns[3].value).toEqual(222)
    expect(rows[1].columns[4].value).toEqual(333333333)
    expect(rows[1].columns[5].value).toEqual(444.555)
    expect(rows[1].columns[6].value).toEqual(4673231456670056448n) // should be 09/21/74 00:00:00
    expect(rows[1].columns[7].value).toEqual('[unknown type]') // should be money 3.5000
    expect(rows[1].columns[8].value).toEqual(1)
    expect(rows[0].columns[0].value).toEqual('a')
    expect(rows[0].columns[1].value).toEqual('b')
    expect(rows[0].columns[2].value).toEqual(0)
    expect(rows[0].columns[3].value).toEqual(0)
    expect(rows[0].columns[4].value).toEqual(0)
    expect(rows[0].columns[5].value).toEqual(0)
    expect(rows[0].columns[6].value).toEqual(4673956859466481664n) // 12/12/81 00:00:00
    expect(rows[0].columns[7].value).toEqual('[unknown type]') // money 0.0000
    expect(rows[0].columns[8].value).toEqual(0)
  })

  it('table 2 should have no rows', () => {
    const rows = jetdb.rows('Table2')
    expect(rows.length).toEqual(0)
  })
})

describe('JetDb 4 stream tests', () => {
  let jetdb: JetDb
  beforeAll(() => {
    jetdb = new JetDb('./tests/data/testV2000.mdb')
  })
  it('table 1 have 2 rows', done => {
    const rowstream = jetdb.streamRows('Table1')
    let count = 0
    rowstream
      .on('data', (rows: []) => {
        count += rows.length
      })
      .on('end', () => {
        try {
          expect(count).toEqual(2)
          done()
        } catch (error) {
          done(error as Error)
        }
      })
  })
  it('table 2 should have no rows', done => {
    const rowstream = jetdb.streamRows('Table2')
    let count = 0
    rowstream
      .on('data', (rows: []) => {
        count += rows.length
      })
      .on('end', () => {
        try {
          expect(count).toEqual(0)
          done()
        } catch (error) {
          done(error as Error)
        }
      })
  })
  it('table 1 columns should have correct values', done => {
    const rowstream = jetdb.streamRows('Table1')
    const rows: Row[] = []
    rowstream
      .on('data', (newRows: Row[]) => {
        rows.push(...newRows)
      })
      .on('end', () => {
        try {
          expect(rows[0].columns[0].value).toEqual('abcdefg')
          expect(rows[0].columns[1].value).toEqual('hijklmnop')
          expect(rows[0].columns[2].value).toEqual(2)
          expect(rows[0].columns[3].value).toEqual(222)
          expect(rows[0].columns[4].value).toEqual(333333333)
          expect(rows[0].columns[5].value).toEqual(444.555)
          expect(rows[0].columns[6].value).toEqual(4673231456670056448n) // should be 09/21/74 00:00:00
          expect(rows[0].columns[7].value).toEqual('[unknown type]') // should be money 3.5000
          expect(rows[0].columns[8].value).toEqual(1)
          expect(rows[1].columns[0].value).toEqual('a')
          expect(rows[1].columns[1].value).toEqual('b')
          expect(rows[1].columns[2].value).toEqual(0)
          expect(rows[1].columns[3].value).toEqual(0)
          expect(rows[1].columns[4].value).toEqual(0)
          expect(rows[1].columns[5].value).toEqual(0)
          expect(rows[1].columns[6].value).toEqual(4673956859466481664n) // 12/12/81 00:00:00
          expect(rows[1].columns[7].value).toEqual('[unknown type]') // money 0.0000
          expect(rows[1].columns[8].value).toEqual(0)
          done()
        } catch (error) {
          done(error as Error)
        }
      })
  })
})
