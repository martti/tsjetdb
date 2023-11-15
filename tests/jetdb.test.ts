import { describe, expect, it, beforeAll } from '@jest/globals'
import { JetDb } from '../src/jetdb'

describe('JetDb tests', () => {
  it('should throw unknown database version', () => {
    const jetdb = new JetDb('tmp')
    expect(() => {
      jetdb['databaseVersion'](
        Buffer.from([
          0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2,
        ]),
      )
    }).toThrow('Unknown database version 2')
  })

  it('should throw unknown database version for JETDB3', () => {
    const jetdb = new JetDb('tmp')
    expect(() => {
      jetdb['databaseVersion'](
        Buffer.from([
          0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        ]),
      )
    }).toThrow('JETDB3 not supported')
  })

  it('should throw unknown database version for JETDB3', async () => {
    await expect(JetDb.create('./tests/data/testV1997.mdb')).rejects.toThrow(
      'JETDB3 not supported',
    )
  })
})

describe('JetDb 4 tests', () => {
  let jetdb: JetDb

  beforeAll(async () => {
    jetdb = await JetDb.create('./tests/data/testV2000.mdb')
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

  it('table 1 have 2 rows', async () => {
    expect((await jetdb.rows('Table1')).length).toEqual(2)
  })

  it('table 1 columns should have correct values', async () => {
    const rows = await jetdb.rows('Table1')
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

  it('table 2 should have no rows', async () => {
    const rows = await jetdb.rows('Table2')
    expect(rows.length).toEqual(0)
  })
})
