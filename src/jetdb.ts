import { Parser } from 'binary-parser'
import { promises as fs } from 'fs'

type BitArray = {
  a: 0 | 1
  b: 0 | 1
  c: 0 | 1
  d: 0 | 1
  e: 0 | 1
  f: 0 | 1
  g: 0 | 1
  h: 0 | 1
}

function readPageBitmap(buf: Buffer, pageStart: number): Array<number> {
  const bitmapParser = new Parser().array('', {
    type: Parser.start()
      .endianness('little')
      .bit1('a')
      .bit1('b')
      .bit1('c')
      .bit1('d')
      .bit1('e')
      .bit1('f')
      .bit1('g')
      .bit1('h'),
    readUntil: 'eof',
  })

  const bits = bitmapParser
    .parse(buf)
    .flatMap((p: BitArray) => Object.values(p))
  let c = pageStart
  const pages: Array<number> = []
  bits.forEach((el: number) => {
    if (el == 1) pages.push(c)
    c += 1
  })
  return pages
}

type usedPagesMap = {
  firstPageApplies: number
}

async function parseUsedPagesMap(
  buf: Buffer,
  // fh: fsPromise.FileHandle,
  reader: BufferReader,
  version: DatabaseConfig,
): Promise<number[]> {
  const usedPagesParser1 = new Parser()
    .endianness('little')
    .seek(14)
    .uint16('firstPageApplies')
    .seek(function (this: usedPagesMap) {
      return this.firstPageApplies - 16
    })
    .uint8('mapType')
    .buffer('bitmap', { readUntil: 'eof' })

  const usedPagesMap = usedPagesParser1.parse(buf)
  // console.log(usedPagesMap)

  const usedPages: Array<number> = []
  if (usedPagesMap.mapType == 1) {
    const pagesParser = new Parser().array('', {
      type: Parser.start().endianness('little').uint32(''),
      readUntil: 'eof',
    })

    const pages = pagesParser.parse(usedPagesMap.bitmap)
    // .filter((p: number) => p > 0)

    const pageParser = new Parser()
      .endianness('little')
      .seek(4)
      .buffer('bitmap', { readUntil: 'eof' })

    for (const [idx, pageNumber] of pages.entries()) {
      // pages.forEach((pageNumber: number, idx: number) => {
      const pagesInPage = (version.pageSize - 4) * 8
      const pageStart = idx * pagesInPage

      if (pageNumber > 0) {
        const page = pageParser.parse(
          await reader.readBuffer(version.pageSize, pageNumber),
        )
        readPageBitmap(page.bitmap, pageStart).forEach((p: number) => {
          usedPages.push(p)
        })
      }
    }
  } else {
    const pageParser = new Parser()
      .endianness('little')
      .uint32('pageStart')
      .buffer('bitmap', { readUntil: 'eof' })
    const page = pageParser.parse(usedPagesMap.bitmap)
    readPageBitmap(page.bitmap, page.pageStart).forEach((p: number) => {
      usedPages.push(p)
    })
  }
  return usedPages
}

type TdefColumn = {
  type: number
  number: number
  offsetF: number
  offsetV: number
  length: number
  bitmask: number
}

type TdefColumnName = {
  length: number
  name: string
}

type Tdef = {
  pagecode: number
  cols: TdefColumn[]
  colNames: TdefColumnName[]
  usedPagesPage: number
}

function parseTdef(buf: Buffer): Tdef {
  const tdef = new Parser()
    .endianness('little')
    .uint8('pagecode', { assert: 0x02 })
    .seek(1)
    .uint16('freeSpaceInPage')
    .uint32('nextPage')
    .uint32('tdefLen')
    .seek(4)
    .uint32('numRows')
    .uint32('autoNumber')
    .uint8('autoNumberFlag')
    .seek(3)
    .uint32('autoNumberValue')
    .seek(8)
    .uint8('tableType')
    .uint16('maxCols')
    .uint16('numVarCols')
    .uint16('numCols')
    .uint32('numIdx')
    .uint32('numRealIdx')
    .uint8('usedPagesRow')
    // .bit24('usedPagesPage')
    // 24 bit uint wrapper
    .wrapped('', {
      length: 3,
      wrapper: function (buffer: Buffer) {
        const tmp = Buffer.alloc(4)
        buffer.copy(tmp)
        return tmp
      },
      type: new Parser().uint32le('usedPagesPage'),
    })
    .uint32('freePagesCount')
    .array('indexes', {
      type: Parser.start()
        .endianness('little')
        .seek(4)
        .uint32('idxRows')
        .seek(4),
      length: 'numRealIdx',
    })
    .array('cols', {
      type: Parser.start()
        .endianness('little')
        .uint8('type')
        .seek(4)
        .uint16('number')
        .uint16('offsetV')
        .uint16('num')
        .uint16('misc')
        .uint16('miscExt')
        .uint8('bitmask')
        .uint8('miscFlags')
        .seek(4)
        .uint16('offsetF')
        .uint16('length'),
      length: 'numCols',
    })
    .array('colNames', {
      type: Parser.start()
        .endianness('little')
        .uint16('length')
        .string('name', { encoding: 'utf-16', length: 'length' }),
      length: 'numCols',
    })
  // .array('realIndexes', {
  //   type: Parser.start()
  //     .endianness('little')
  //     .seek(4)
  //     .array('columns', {
  //       type: Parser.start()
  //         .endianness('little')
  //         .uint16('number')
  //         .uint8('order'),
  //       length: 10,
  //     })
  //     .uint32('usedPages')
  //     .uint32('firstDp')
  //     .uint8('flags')
  //     .seek(9),
  //   length: 'numRealIdx',
  // })
  // .array('indexInfo', {
  //   type: Parser.start()
  //     .endianness('little')
  //     .seek(4)
  //     .uint32('number')
  //     .uint32('number2')
  //     .uint8('relTblType')
  //     .uint32('relIdxNum')
  //     .uint32('relTblPage')
  //     .uint8('cascadeUps')
  //     .uint8('cascadeDels')
  //     .uint8('type')
  //     .seek(4),
  //   length: 'numIdx',
  // })
  // .array('indexNames', {
  //   type: Parser.start()
  //     .endianness('little')
  //     .uint16('length')
  //     .string('name', { encoding: 'utf-16', length: 'length' }),
  //   length: 'numIdx',
  // })
  // .array('freePages', {
  //   type: Parser.start()
  //     .endianness('little')
  //     .uint16('colNum')
  //     .uint32('usedPages')
  //     .uint32('freePages'),
  //   readUntil: function (item, buffer) {
  //     // stop before 0xffff
  //     // console.log(buffer)
  //     // if (buffer.length == 2) return true
  //     if (item.colNum == 0xffff) return true
  //     return buffer[0] == 255 && buffer[1] == 255
  //   },
  // })
  // console.log(buf)
  return tdef.parse(buf) as Tdef
}

type DatabaseConfig = {
  pageSize: number
  version: number
}

type DataPage = {
  pagecode: 1
  freeSpaceInPage: number
  tdefPage: number
  numRows: number
  offsets: number[]
}

type Row = {
  number: number
  columns: ColumnData[]
}

type RowOffset = {
  lookupFlag: number
  delFlag: boolean
  offset: number
  next: number
}

type ColumnData = {
  position: number
  rawValue: Buffer
  type: number
  name: string
  isNull: boolean
  value: number | bigint | string | Date | null
}

async function readRows(
  usedPagesMap: number[],
  reader: BufferReader,
  version: DatabaseConfig,
  schema: Tdef,
): Promise<Row[]> {
  const rowData: Row[] = []

  for (const pageNumber of usedPagesMap) {
    const buffer = await reader.readBuffer(version.pageSize, pageNumber)
    const parser = new Parser()
      .endianness('little')
      .uint8('pagecode', { assert: 0x01 })
      .seek(1)
      .uint16('freeSpaceInPage')
      .uint32('tdefPage')
      .seek(4)
      .uint16('numRows')
      .array('offsets', {
        type: Parser.start().endianness('little').uint16(''),
        length: 'numRows',
      })
    const parsed = parser.parse(buffer) as DataPage
    // as Testi
    // console.log(parsed)
    // const offsets: Array<any> = []
    const offsets = parsed.offsets.map((os: number, idx: number) => {
      // console.log(offset.offsetRow)
      let next = version.pageSize
      if (idx > 0) next = parsed.offsets[idx - 1] & 0x1fff
      // const lookupFlag = (offset.offsetRow >> 8) & 0xff
      const lookupFlag = os & (0x8000 >>> 0)
      const delFlag = (os & (0x4000 >>> 0)) != 0
      // const delFlag = (offset.offsetRow >> 8) & 0x80
      const offset = (os & 0x1fff) >>> 0
      // console.log(os, offset, os.toString(2), offset.toString(2))
      // console.log(lookupFlag, delFlag, offset, next)
      return { lookupFlag, delFlag, offset, next } as RowOffset
    })
    // console.log(offsets)

    offsets
      .filter(p => !p.delFlag) // filter deleted rows
      .forEach((offset: RowOffset, idx: number) => {
        // fixed length columns
        // var_columns
        // eod
        // var_table
        // if (offset.delFlag) return
        // const p2 = new Parser().seek(offset.offset).uint16le('')
        // const columnsInRow = p2.parse(xx)
        const columnsInRow = buffer.readUInt16LE(offset.offset)
        const columnData: ColumnData[] = []

        const varLenSize = 2
        const nullMaskSize = Math.floor((columnsInRow + 7) / 8)

        const varLen = buffer.readUInt16LE(
          offset.next - varLenSize - nullMaskSize,
        )
        // const p3 = new Parser()
        //   .seek(offset.next - varLenSize - nullMaskSize)
        //   .uint16le('')
        // const varLen = p3.parse(xx)

        const p4 = new Parser()
          .seek(offset.next - varLenSize - nullMaskSize - varLen * varLenSize)
          .array('', {
            type: 'uint16le',
            length: varLen,
          })
        const varOffsets = p4.parse(buffer).reverse()
        // console.log(varOffsets)

        // end of varoffsets position to make last column.offsetV + 1 work
        // should be read by previous parser and not here?
        varOffsets.push(
          buffer.readUInt16LE(
            offset.next -
              varLenSize -
              nullMaskSize -
              varLenSize * varLen -
              varLenSize,
          ),
        )

        const p8 = new Parser().seek(offset.next - nullMaskSize).array('', {
          type: Parser.start()
            .endianness('little')
            .bit1('a')
            .bit1('b')
            .bit1('c')
            .bit1('d')
            .bit1('e')
            .bit1('f')
            .bit1('g')
            .bit1('h'),
          length: nullMaskSize,
        })
        const nullMask = p8
          .parse(buffer)
          .flatMap((p: BitArray) => Object.values(p))

        schema.cols.forEach((column: TdefColumn, idx: number) => {
          // fixed
          // if ((column.bitmask & (0x02 >>> 0)) == 2) {
          //   console.log('NULLABLE')
          // }
          // console.log(schema.colNames[idx].name, column.offsetV, column.type)

          let start = 0
          let length = 0
          if ((column.bitmask & 0x01) == 1) {
            // fixed
            start = offset.offset + column.offsetF + varLenSize
            length = column.length
          } else {
            // var
            start = offset.offset + varOffsets[column.offsetV]
            if (column.offsetV + 1 in varOffsets) {
              length =
                varOffsets[column.offsetV + 1] - varOffsets[column.offsetV]
            }
          }

          // decompress_unicode(const char *src, size_t slen, char *dst, size_t dlen) {
          // decompress_unicode(src + 2, slen - 2, tmp, slen * 2);

          // compressed format
          // allocate double the original
          // add null to every other byte
          // unless there exists null byte then
          // adding null bytes is switched off until next null byte comes
          function decompressUnicode(src: Buffer, slen: number, dlen: number) {
            let compress = 1
            let pos = 0
            let tlen = 0
            const dst = Buffer.alloc(slen * 2)
            while (slen > 0 && tlen < dlen) {
              if (src[pos] == 0x00) {
                compress = compress ? 0 : 1
                pos += 1
                slen -= 1
              } else if (compress) {
                dst[tlen] = src[pos]
                tlen += 1
                dst[tlen] = 0x00
                tlen += 1
                slen -= 1
                pos += 1
              } else if (slen >= 2) {
                dst[tlen] = src[pos]
                tlen += 1
                pos += 1
                dst[tlen] = src[pos]
                tlen += 1
                pos += 1
                slen -= 2
              } else {
                break
              }
            }
            // console.log(src)
            // console.log(tlen)
            // console.log(dst.subarray(0, tlen))
            return dst.subarray(0, tlen)

            // unsigned int compress=1;
            // size_t tlen = 0;
            // while (slen > 0 && tlen < dlen) {
            //   if (*src == 0) {
            //     compress = (compress) ? 0 : 1;
            //     src++;
            //     slen--;
            //   } else if (compress) {
            //     dst[tlen++] = *src++;
            //     dst[tlen++] = 0;
            //     slen--;
            //   } else if (slen >= 2){
            //     dst[tlen++] = *src++;
            //     dst[tlen++] = *src++;
            //     slen-=2;
            //   } else { // Odd # of bytes
            //     break;
            //   }
            // }
            // return tlen;
          }

          if (length > 0) {
            let columnValue = null
            switch (column.type) {
              case 1: // bool
                columnValue = buffer.readUint8(start) ? 1 : 0
                break
              case 2: // byte
                columnValue = buffer.readUint8(start)
                break
              case 3: // int
                columnValue = buffer.readUint16LE(start)
                break
              case 4: // longint
                columnValue = buffer.readUint32LE(start)
                break
              case 7: // double
                columnValue = buffer.readDoubleLE(start)
                break
              case 8: // datetime
                columnValue = buffer.readBigUInt64LE(start)
                break
              case 10: // text
                // compressed format
                if (buffer[start] == 0xff && buffer[start + 1] == 0xfe) {
                  start = start + 2
                  const slen = start + length - start - 2
                  const decompressed = decompressUnicode(
                    buffer.subarray(start, start + length - 2),
                    slen,
                    slen * 2,
                  )
                  columnValue = decompressed.toString('utf16le')
                  // console.log(columnValue)
                } else {
                  columnValue = buffer.toString(
                    'utf16le',
                    start,
                    start + length,
                  )
                  // console.log('COLVAL')
                  // console.log(columnValue)
                }
                // columnValue = new Parser()
                //   .endianness('little')
                //   .seek(start)
                //   .string('', { encoding: 'utf-16', length: length })
                //   .parse(xx)
                break
              default:
                columnValue = '[unknown type]'
            }
            columnData.push({
              rawValue: Buffer.from([]),
              position: column.number,
              name: schema.colNames[idx].name,
              type: column.type,
              value: columnValue,
              isNull: nullMask[column.number] == 0,
            })
          } else {
            columnData.push({
              rawValue: Buffer.from([]),
              position: column.number,
              name: schema.colNames[idx].name,
              type: column.type,
              value: nullMask[column.number] == 0 ? null : '',
              isNull: nullMask[column.number] == 0,
            })
          }
        })
        rowData.push({ columns: columnData, number: idx } as Row)
      })
  }
  return rowData
}

class BufferReader {
  private fh!: fs.FileHandle

  constructor() {}

  public static async create(filename: string): Promise<BufferReader> {
    const reader = new BufferReader()
    reader.fh = await fs.open(filename, 'r')
    return reader
  }

  async readBuffer(size: number, position: number): Promise<Buffer> {
    const buf = Buffer.alloc(size)
    await this.fh.read(buf, 0, size, size * position)
    // console.log(buf)
    return buf
  }
}

export class JetDb {
  private reader!: BufferReader
  private filename: string
  private userTables: Row[]
  private schema: Tdef[]
  private version!: DatabaseConfig

  constructor(filename: string) {
    this.filename = filename
    this.userTables = []
    this.schema = []
  }

  public static async create(filename: string): Promise<JetDb> {
    const jetdb = new JetDb(filename)
    await jetdb.init()
    return jetdb
  }

  private databaseVersion(buf: Buffer): DatabaseConfig {
    switch (buf.readInt8(0x14)) {
      case 0x00: {
        throw new Error('JETDB3 not supported')
        // return { pageSize: 2048, version: 3 } as DatabaseConfig
      }
      case 0x01: {
        return { pageSize: 4096, version: 4 } as DatabaseConfig
      }
      default:
        throw new Error(`Unknown database version ${buf.readInt8(0x14)}`)
    }
  }

  private isUserTable(row: Row): boolean {
    const type = row.columns.find((p: ColumnData) => p.name == 'Type')?.value
    const flags = row.columns.find((p: ColumnData) => p.name == 'Flags')?.value
    if (typeof type == 'number' && typeof flags == 'number') {
      if ((type & 0x00ffffff) == 1 && (flags & 0x80000002) == 0) {
        return true
      }
    }
    return false
  }

  private async init() {
    //const buf = Buffer.alloc(1)
    this.reader = await BufferReader.create(this.filename)

    const version = this.databaseVersion(await this.reader.readBuffer(2048, 0))
    this.version = version

    const schema = parseTdef(await this.reader.readBuffer(version.pageSize, 2))

    const usedPagesMap = await parseUsedPagesMap(
      await this.reader.readBuffer(version.pageSize, schema.usedPagesPage),
      this.reader,
      version,
    )

    const rows = await readRows(usedPagesMap, this.reader, version, schema)
    this.userTables = rows.filter((p: Row) => this.isUserTable(p))
    for (const p of this.userTables) {
      const tableId = p.columns.find((p: ColumnData) => p.name == 'Id')?.value
      if (typeof tableId == 'number') {
        this.schema.push(
          parseTdef(await this.reader.readBuffer(version.pageSize, tableId)),
        )
      }
    }
  }

  public tables(): string[] {
    return this.userTables.map((p: Row) => {
      const val = p.columns.find((p: ColumnData) => p.name == 'Name')?.value
      if (typeof val == 'string') {
        return val
      } else {
        throw new Error('Error in table names')
      }
    })
  }

  public columns(table: string): string[] {
    const tableIndex = this.userTables.findIndex((p: Row) => {
      const val = p.columns.find((p: ColumnData) => p.name == 'Name')?.value
      return val == table
    })
    return this.schema[tableIndex].colNames.map((p: TdefColumnName) => {
      return p.name
    })
  }

  public async rows(table: string): Promise<Row[]> {
    const tableIndex = this.userTables.findIndex((p: Row) => {
      const val = p.columns.find((p: ColumnData) => p.name == 'Name')?.value
      return val == table
    })

    const usedPagesMap = await parseUsedPagesMap(
      await this.reader.readBuffer(
        this.version.pageSize,
        this.schema[tableIndex].usedPagesPage,
      ),
      this.reader,
      this.version,
    )

    const rows = await readRows(
      usedPagesMap,
      this.reader,
      this.version,
      this.schema[tableIndex],
    )

    return rows
  }
}