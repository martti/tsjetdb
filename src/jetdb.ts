import { Parser } from 'binary-parser'
import * as fs from 'fs'
import { Transform, Stream } from 'stream'

enum Version {
  JETDB3 = 0,
  JETDB4 = 1,
}

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

type usedPagesMap = {
  firstPageApplies: number
}

type TdefColumn = {
  type: ColumnType
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
  numRows: number
  pagecode: number
  cols: TdefColumn[]
  colNames: TdefColumnName[]
  usedPagesPage: number
  nextPage: number
}

type DatabaseConfig = {
  pageSize: number
  version: Version
}

type DataPage = {
  pagecode: 1
  freeSpaceInPage: number
  tdefPage: number
  numRows: number
  offsets: number[]
}

export type Row = {
  number: number
  columns: ColumnData[]
}

type RowOffset = {
  lookupFlag: number
  delFlag: boolean
  offset: number
  next: number
}

export enum ColumnType {
  BOOL = 0x01 /* Boolean         ( 1 bit ) */,
  BYTE = 0x02 /* Byte            ( 8 bits) */,
  INT = 0x03 /* Integer         (16 bits) */,
  LONGINT = 0x04 /* Long Integer    (32 bits) */,
  MONEY = 0x05 /* Currency        (64 bits) */,
  FLOAT = 0x06 /* Single          (32 bits) */,
  DOUBLE = 0x07 /* Double          (64 bits) */,
  DATETIME = 0x08 /* Date/Time       (64 bits) */,
  BINARY = 0x09 /* Binary        (255 bytes) */,
  TEXT = 0x0a /* Text          (255 bytes) */,
  OLE = 0x0b /* OLE = Long binary */,
  MEMO = 0x0c /* Memo = Long text*/,
  UNKNOWN_0D = 0x0d,
  UNKNOWN_0E = 0x0e,
  REPID = 0x0f /* GUID */,
  NUMERIC = 0x10 /* Scaled decimal  (17 bytes) */,
}

export type ColumnValue = number | bigint | string | Date | null
export type ColumnData = {
  position: number
  rawValue: Buffer
  type: ColumnType
  name: string
  isNull: boolean
  value: ColumnValue
}

class BufferReader implements Disposable {
  #fh: number
  // private filename: string

  constructor(filename: string) {
    this.#fh = fs.openSync(filename, 'r')
    // this.filename = filename
  }

  [Symbol.dispose]() {
    // Close the file
    console.log('CLOSING FILE')
    fs.closeSync(this.#fh)
  }
  // public static async create(filename: string): Promise<BufferReader> {
  //   const reader = new BufferReader()
  //   reader.fh = await fs.promises.open(filename, 'r')
  //   return reader
  // }

  public close() {
    fs.closeSync(this.#fh)
  }

  public readBuffer(size: number, position: number): Buffer {
    const buf = Buffer.alloc(size)
    fs.readSync(this.#fh, buf, 0, size, size * position)
    // console.log(buf)
    return buf
  }
}

export class JetDb {
  private reader: BufferReader
  private filename: string
  private userTables: Row[]
  private schema: Tdef[]
  private config!: DatabaseConfig

  constructor(filename: string) {
    this.filename = filename
    this.userTables = []
    this.schema = []
    this.reader = new BufferReader(this.filename)
    this.config = this.databaseConfig(this.reader.readBuffer(2048, 0))
    this.init()
  }

  public close() {
    this.reader.close()
  }

  private databaseConfig(buf: Buffer): DatabaseConfig {
    switch (buf.readInt8(0x14)) {
      case Version.JETDB3: {
        return { pageSize: 2048, version: Version.JETDB3 } as DatabaseConfig
      }
      case Version.JETDB4: {
        return { pageSize: 4096, version: Version.JETDB4 } as DatabaseConfig
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

  private readPageBitmap(buf: Buffer, pageStart: number): Array<number> {
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

  private parseTdef(buf: Buffer): Tdef {
    const frontParser = new Parser()
      .endianness('little')
      .uint8('pagecode', { assert: 0x02 })
      .seek(1)

    if (this.config.version == Version.JETDB3) {
      frontParser.string('mark', { assert: 'VC', length: 2 })
    } else {
      frontParser.uint16('freeSpaceInPage')
    }
    frontParser.uint32('nextPage').uint32('tdefLen')

    // handle nextpage
    // only supports one nextPage
    // just add nextpage at the end of current buffer
    const tdefFront = frontParser.parse(buf)
    let tdefBuffer = Buffer.alloc(this.config.pageSize)
    if (tdefFront.nextPage > 0) {
      tdefBuffer = Buffer.alloc(this.config.pageSize * 2)
      const nextBuffer = this.reader.readBuffer(
        this.config.pageSize,
        tdefFront.nextPage,
      )
      buf.copy(tdefBuffer)
      // 8 is page header like tdefFront, probably 8 in jetdb3 and 4
      nextBuffer.copy(tdefBuffer, this.config.pageSize, 8)
    } else {
      buf.copy(tdefBuffer)
    }

    // Parse all pages
    const tdefParser = new Parser()
      .endianness('little')
      .uint8('pagecode', { assert: 0x02 })
      .seek(1)

    if (this.config.version == Version.JETDB3) {
      tdefParser.string('mark', { assert: 'VC', length: 2 })
    } else {
      tdefParser.uint16('freeSpaceInPage')
    }
    tdefParser.uint32('nextPage').uint32('tdefLen')

    if (this.config.version == Version.JETDB4) {
      tdefParser.seek(4)
    }

    tdefParser.uint32('numRows').uint32('autoNumber')

    if (this.config.version == Version.JETDB4) {
      tdefParser
        .uint8('autoNumberFlag')
        .seek(3)
        .uint32('autoNumberValue')
        .seek(8)
    }

    tdefParser
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

    if (this.config.version == Version.JETDB3) {
      tdefParser.array('indexes', {
        type: Parser.start().endianness('little').seek(4).uint32('idxRows'),
        length: 'numRealIdx',
      })
    } else {
      tdefParser.array('indexes', {
        type: Parser.start()
          .endianness('little')
          .seek(4)
          .uint32('idxRows')
          .seek(4),
        length: 'numRealIdx',
      })
    }

    const columnParser = Parser.start().endianness('little').uint8('type')

    if (this.config.version == Version.JETDB4) {
      columnParser.seek(4)
    }

    columnParser.uint16('number').uint16('offsetV').uint16('num')

    if (this.config.version == Version.JETDB3) {
      columnParser.uint16('sortOrder')
    }

    columnParser
      .uint16('misc')
      .uint16('miscExt') // unknown on jet3
      .uint8('bitmask')

    if (this.config.version == Version.JETDB4) {
      columnParser.uint8('miscFlags').seek(4)
    }

    columnParser.uint16('offsetF').uint16('length')

    tdefParser.array('cols', {
      type: columnParser,
      length: 'numCols',
    })

    if (this.config.version == Version.JETDB3) {
      tdefParser.array('colNames', {
        type: Parser.start()
          .endianness('little')
          .uint8('length')
          .string('name', { encoding: 'latin1', length: 'length' }),
        length: 'numCols',
      })
    } else {
      tdefParser.array('colNames', {
        type: Parser.start()
          .endianness('little')
          .uint16('length')
          .string('name', { encoding: 'utf-16', length: 'length' }),
        length: 'numCols',
      })
    }

    if (this.config.version == Version.JETDB3) {
      tdefParser.array('realIndexes', {
        type: Parser.start()
          .endianness('little')
          .array('columns', {
            type: Parser.start()
              .endianness('little')
              .uint16('number')
              .uint8('order'),
            length: 10,
          })
          .uint32('usedPages')
          .uint32('firstDp')
          .uint8('flags'),
        length: 'numRealIdx',
      })
    } else {
      tdefParser.array('realIndexes', {
        type: Parser.start()
          .endianness('little')
          .seek(4)
          .array('columns', {
            type: Parser.start()
              .endianness('little')
              .uint16('number')
              .uint8('order'),
            length: 10,
          })
          .uint32('usedPages')
          .uint32('firstDp')
          .uint8('flags')
          .seek(9),
        length: 'numRealIdx',
      })
    }

    if (this.config.version == Version.JETDB3) {
      tdefParser
        .array('indexInfo', {
          type: Parser.start()
            .endianness('little')
            .uint32('number')
            .uint32('number2')
            .uint8('relTblType')
            .uint32('relIdxNum')
            .uint32('relTblPage')
            .uint8('cascadeUps')
            .uint8('cascadeDels')
            .uint8('type'),
          length: 'numIdx',
        })
        .array('indexNames', {
          type: Parser.start()
            .endianness('little')
            .uint8('length')
            .string('name', { encoding: 'latin1', length: 'length' }),
          length: 'numIdx',
        })
        .array('freePages', {
          type: Parser.start()
            .endianness('little')
            .uint16('colNum')
            .uint32('usedPages')
            .uint32('freePages'),
          readUntil: function (item, buffer) {
            // stop before 0xffff
            // console.log(buffer)
            // if (buffer.length == 2) return true
            if (item.colNum == 0xffff) return true
            return buffer[0] == 255 && buffer[1] == 255
          },
        })
    } else {
      tdefParser
        .array('indexInfo', {
          type: Parser.start()
            .endianness('little')
            .seek(8)
            .uint32('number')
            .uint32('number2')
            .uint8('relTblType')
            .uint32('relIdxNum')
            .uint32('relTblPage')
            .uint8('cascadeUps')
            .uint8('cascadeDels')
            .uint8('type'),
          length: 'numIdx',
        })
        .array('indexNames', {
          type: Parser.start()
            .endianness('little')
            .uint16('length')
            .string('name', { encoding: 'utf-16', length: 'length' }),
          length: 'numIdx',
        })
        .array('freePages', {
          type: Parser.start()
            .endianness('little')
            .uint16('colNum')
            .uint32('usedPages')
            .uint32('freePages'),
          readUntil: function (item, buffer) {
            // stop before 0xffff
            // console.log(buffer)
            // if (buffer.length == 2) return true
            if (item.colNum == 0xffff) return true
            return buffer[0] == 255 && buffer[1] == 255
          },
        })
    }
    // console.log(buf)

    const tdef = tdefParser.parse(tdefBuffer) as Tdef
    return tdef
  }

  private parseUsedPagesMap(buf: Buffer): number[] {
    let skip = 14
    if (this.config.version == Version.JETDB3) {
      skip = 10
    }
    const usedPagesParser1 = new Parser()
      .endianness('little')
      .seek(skip)
      .uint16('firstPageApplies')
      .seek(function (this: usedPagesMap) {
        return this.firstPageApplies - (skip + 2)
      })
      .uint8('mapType')
      .buffer('bitmap', { readUntil: 'eof' })

    const usedPagesMap = usedPagesParser1.parse(buf)

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
        const pagesInPage = (this.config.pageSize - 4) * 8
        const pageStart = idx * pagesInPage

        if (pageNumber > 0) {
          const page = pageParser.parse(
            this.reader.readBuffer(this.config.pageSize, pageNumber),
          )
          this.readPageBitmap(page.bitmap, pageStart).forEach((p: number) => {
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
      this.readPageBitmap(page.bitmap, page.pageStart).forEach((p: number) => {
        usedPages.push(p)
      })
    }
    return usedPages
  }

  // buffer should be final text buffer
  private parseText(buffer: Buffer): string {
    if (this.config.version == Version.JETDB3) {
      // default encoding for jet3 should be cp1252
      // but it depends on the machine the file was created
      // latin1 may be close enough
      return buffer.toString('latin1')
    } else {
      // compressed format
      if (buffer[0] == 0xff && buffer[1] == 0xfe) {
        const start = 2
        const slen = buffer.length - 2
        const decompressed = this.decompressUnicode(
          buffer.subarray(start, buffer.length),
          slen,
          slen * 2,
        )
        return decompressed.toString('utf16le')
      } else {
        return buffer.toString('utf16le')
      }
    }
  }

  // decompress_unicode(const char *src, size_t slen, char *dst, size_t dlen) {
  // decompress_unicode(src + 2, slen - 2, tmp, slen * 2);
  // compressed format
  // allocate double the original
  // add null to every other byte
  // unless there exists null byte then
  // adding null bytes is switched off until next null byte comes
  private decompressUnicode(src: Buffer, slen: number, dlen: number) {
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

  private parseColumn(
    buffer: Buffer,
    column: TdefColumn,
    start: number,
    length: number,
  ): number | bigint | string | Date | null {
    switch (column.type) {
      case 1: // bool
        return buffer.readUint8(start) ? 1 : 0
      case 2: // byte
        return buffer.readUint8(start)
      case 3: // int
        return buffer.readUint16LE(start)
      case 4: // longint
        return buffer.readUint32LE(start)
      case 7: // double
        return buffer.readDoubleLE(start)
      case 8: // datetime
        // parseDate(buffer.readDoubleLE(start))
        return buffer.readBigUInt64LE(start)
      case 10: // text
        return this.parseText(buffer.subarray(start, start + length))
      case 12: {
        // memo, long text
        let memoLen = buffer.readUint16LE(start)
        // memoLen << 8
        memoLen += buffer.readUint8(start + 2)
        const memoMask = buffer.readUint8(start + 3)
        const tmp = buffer.readUint32LE(start + 4)
        const memoPage = tmp >> 8
        const memoRow = tmp & 0xff
        // const memoRow = buffer.readUint8(start + 4)
        // let memoPage = buffer.readUint16LE(start + 5)
        // memoPage << 8
        // memoPage += buffer.readUint8(start + 7)

        // console.log(memoLen, memoRow, memoMask, memoPage)
        // inline text
        if (memoMask == 0x80) {
          // 12 bytes from start
          return this.parseText(
            buffer.subarray(start + 12, start + 12 + memoLen),
          )
        } else if (memoMask == 0x40) {
          // column is in page memoPage (LVAL)
          // open page buffer
          // parse offset map
          // take offset at memoRow
          // memo size is start offset -> end offset
          // parseText(buffer.subarray(start, end))
          const memoBuffer = this.reader.readBuffer(
            this.config.pageSize,
            memoPage,
          )
          const offsets = this.parseOffsets(memoBuffer)
          return this.parseText(
            memoBuffer.subarray(offsets[memoRow].offset, offsets[memoRow].next),
          )
          // return '[unknown type]'
        } else if (memoMask == 0x00) {
          // from LVAL page type 2
          return '[unknown type]'
        }

        return '[unknown type]'
      }
      default:
        // console.log(
        //   'UNKNOWN',
        //   column.type,
        //   buffer.subarray(start, start + length),
        // )
        return '[unknown type]'
    }
  }

  private readRowsForPage(buffer: Buffer, schema: Tdef): Row[] {
    const rowData: Row[] = []

    const offsets = this.parseOffsets(buffer)
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
        let columnsInRow = 0
        if (this.config.version == Version.JETDB3) {
          columnsInRow = buffer.readUInt8(offset.offset)
        } else {
          columnsInRow = buffer.readUInt16LE(offset.offset)
        }
        const columnData: ColumnData[] = []

        let varLenSize = 2
        if (this.config.version == Version.JETDB3) {
          varLenSize = 1
        }

        const nullMaskSize = Math.floor((columnsInRow + 7) / 8)

        let varLen = 0
        if (this.config.version == Version.JETDB3) {
          varLen = buffer.readUInt8(offset.next - varLenSize - nullMaskSize)
        } else {
          varLen = buffer.readUInt16LE(offset.next - varLenSize - nullMaskSize)
        }

        // const p3 = new Parser()
        //   .seek(offset.next - varLenSize - nullMaskSize)
        //   .uint16le('')
        // const varLen = p3.parse(xx)

        // console.log(varLen, columnsInRow)

        let varLenType = 'uint16le'
        if (this.config.version == Version.JETDB3) {
          varLenType = 'uint8'
        }
        const p4 = new Parser()
          .seek(offset.next - varLenSize - nullMaskSize - varLen * varLenSize)
          .array('', {
            type: varLenType,
            length: varLen,
          })
        const varOffsets = p4.parse(buffer).reverse()
        // console.log(varOffsets)

        // end of varoffsets position to make last column.offsetV + 1 work
        // should be read by previous parser and not here?
        if (this.config.version == Version.JETDB3) {
          varOffsets.push(
            buffer.readUInt8(
              offset.next -
                varLenSize -
                nullMaskSize -
                varLenSize * varLen -
                varLenSize,
            ),
          )
        } else {
          varOffsets.push(
            buffer.readUInt16LE(
              offset.next -
                varLenSize -
                nullMaskSize -
                varLenSize * varLen -
                varLenSize,
            ),
          )
        }

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

          let columnValue: ColumnValue =
            nullMask[column.number] == 0 ? null : ''
          if (length > 0) {
            columnValue = this.parseColumn(buffer, column, start, length)
          }
          columnData.push({
            rawValue: buffer.subarray(start, start + length),
            position: column.number,
            name: schema.colNames[idx].name,
            type: column.type,
            value: columnValue,
            isNull: nullMask[column.number] == 0,
          })
        })

        rowData.push({ columns: columnData, number: idx } as Row)
      })

    return rowData
  }

  private parseOffsets(buffer: Buffer) {
    const parser = new Parser()
      .endianness('little')
      .uint8('pagecode', { assert: 0x01 })
      .seek(1)
      .uint16('freeSpaceInPage')
      .uint32('tdefPage')
    if (this.config.version == Version.JETDB4) {
      parser.seek(4)
    }
    parser.uint16('numRows').array('offsets', {
      type: Parser.start().endianness('little').uint16(''),
      length: 'numRows',
    })
    const parsed = parser.parse(buffer) as DataPage
    // as Testi
    // console.log(parsed)
    // const offsets: Array<any> = []
    const offsets = parsed.offsets.map((os: number, idx: number) => {
      // console.log(offset.offsetRow)
      let next = this.config.pageSize
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
    return offsets
  }

  private readRows(
    usedPagesMap: number[],
    reader: BufferReader,
    schema: Tdef,
  ): Row[] {
    const rowData: Row[] = []
    for (const pageNumber of usedPagesMap) {
      const buffer = reader.readBuffer(this.config.pageSize, pageNumber)
      const rows = this.readRowsForPage(buffer, schema)
      rowData.push(...rows)
    }
    return rowData
  }

  private async init() {
    const schema = this.parseTdef(
      this.reader.readBuffer(this.config.pageSize, 2),
    ) as Tdef

    const usedPagesMap = this.parseUsedPagesMap(
      this.reader.readBuffer(this.config.pageSize, schema.usedPagesPage),
    )

    const rows = this.readRows(usedPagesMap, this.reader, schema)
    this.userTables = rows.filter((p: Row) => this.isUserTable(p))
    for (const p of this.userTables) {
      const tableId = p.columns.find((p: ColumnData) => p.name == 'Id')?.value
      if (typeof tableId == 'number') {
        const tdef = this.parseTdef(
          this.reader.readBuffer(this.config.pageSize, tableId),
        ) as Tdef
        this.schema.push(tdef)
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

  public columnsWithType(table: string): [string, ColumnType][] {
    const tableIndex = this.userTables.findIndex((p: Row) => {
      const val = p.columns.find((p: ColumnData) => p.name == 'Name')?.value
      return val == table
    })
    return this.schema[tableIndex].cols.map((p: TdefColumn, index: number) => {
      return [this.schema[tableIndex].colNames[index].name, p.type]
    })
  }

  public tablesWithRows(): [string, number][] {
    return this.userTables.map((p: Row, index: number) => {
      const val = p.columns.find((p: ColumnData) => p.name == 'Name')?.value
      if (typeof val == 'string') {
        return [val, this.schema[index].numRows]
      } else {
        throw new Error('Error in table names')
      }
    })
  }

  public rows(table: string): Row[] {
    const tableIndex = this.userTables.findIndex((p: Row) => {
      const val = p.columns.find((p: ColumnData) => p.name == 'Name')?.value
      return val == table
    })

    const usedPagesMap = this.parseUsedPagesMap(
      this.reader.readBuffer(
        this.config.pageSize,
        this.schema[tableIndex].usedPagesPage,
      ),
    )

    const rows = this.readRows(
      usedPagesMap,
      this.reader,
      this.schema[tableIndex],
    )

    return rows
  }

  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  private pickPages = (pages: number[], _pageSize: number) => {
    let p = 0
    return new Transform({
      transform(chunk, _encoding, cb) {
        try {
          // version 1:
          // highwatermark is default
          // so we read larger chunks at a time and have process them
          // const pagesOnChunk = pages.filter((pn: number) => {
          //   return pn >= p && pn < p + chunk.length / pageSize
          // })
          // // console.log(pagesOnChunk)
          // for (const pn of pagesOnChunk) {
          //   // console.log((pn - p) * pageSize, (pn - p) * pageSize + pageSize)
          //   this.push(chunk.subarray((pn - p) * pageSize, (pn - p) * pageSize + pageSize))
          //   // chunk.subarray()
          // }
          // p += chunk.length / pageSize
          // version 2:
          // highwatermark is pageSize and chunks come as one page at a time
          if (pages.includes(p)) {
            this.push(chunk)
          }
          p += 1
          cb()
        } catch (error) {
          cb(error as Error)
        }
      },
    })
  }

  private transformPages = (jetdb: JetDb, schema: Tdef) => {
    return new Transform({
      readableObjectMode: true,
      transform(buffer, _encoding, cb) {
        const rows = jetdb.readRowsForPage.call(jetdb, buffer, schema)
        this.push(rows)
        cb()
      },
    })
  }

  public streamRows(table: string): Stream {
    const tableIndex = this.userTables.findIndex((p: Row) => {
      const val = p.columns.find((p: ColumnData) => p.name == 'Name')?.value
      return val == table
    })

    const usedPagesMap = this.parseUsedPagesMap(
      this.reader.readBuffer(
        this.config.pageSize,
        this.schema[tableIndex].usedPagesPage,
      ),
    )

    const fileStream = fs.createReadStream(this.filename, {
      highWaterMark: this.config.pageSize,
    })

    return fileStream
      .pipe(this.pickPages(usedPagesMap, this.config.pageSize))
      .pipe(this.transformPages(this, this.schema[tableIndex]))
  }
}
