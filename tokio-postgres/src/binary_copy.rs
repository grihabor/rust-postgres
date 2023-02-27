//! Utilities for working with the PostgreSQL binary copy format.

use crate::types::{FromSql, IsNull, ToSql, Type, WrongType};
use crate::{slice_iter, CopyInSink, CopyOutStream, Error};
use byteorder::{BigEndian, ByteOrder};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use futures_util::{ready, SinkExt, Stream, stream_select};
use pin_project_lite::pin_project;
use postgres_types::{BorrowToSql, Field};
use std::convert::TryFrom;
use tokio::io;
use std::io::Cursor;
use std::ops::{Bound, Range};
use std::pin::Pin;
use std::future::Future;
use std::mem;
use std::slice::SliceIndex;
use fallible_iterator::FallibleIterator;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncReadExt, ReadBuf};
use std::marker::PhantomData;

const MAGIC: &[u8] = b"PGCOPY\n\xff\r\n\0";
const HEADER_LEN: usize = MAGIC.len() + 4 + 4;

pin_project! {
    /// A type which serializes rows into the PostgreSQL binary copy format.
    ///
    /// The copy *must* be explicitly completed via the `finish` method. If it is not, the copy will be aborted.
    pub struct BinaryCopyInWriter {
        #[pin]
        sink: CopyInSink<Bytes>,
        types: Vec<Type>,
        buf: BytesMut,
    }
}

impl BinaryCopyInWriter {
    /// Creates a new writer which will write rows of the provided types to the provided sink.
    pub fn new(sink: CopyInSink<Bytes>, types: &[Type]) -> BinaryCopyInWriter {
        let mut buf = BytesMut::new();
        buf.put_slice(MAGIC);
        buf.put_i32(0); // flags
        buf.put_i32(0); // header extension

        BinaryCopyInWriter {
            sink,
            types: types.to_vec(),
            buf,
        }
    }

    /// Writes a single row.
    ///
    /// # Panics
    ///
    /// Panics if the number of values provided does not match the number expected.
    pub async fn write(self: Pin<&mut Self>, values: &[&(dyn ToSql + Sync)]) -> Result<(), Error> {
        self.write_raw(slice_iter(values)).await
    }

    /// A maximally-flexible version of `write`.
    ///
    /// # Panics
    ///
    /// Panics if the number of values provided does not match the number expected.
    pub async fn write_raw<P, I>(self: Pin<&mut Self>, values: I) -> Result<(), Error>
        where
            P: BorrowToSql,
            I: IntoIterator<Item=P>,
            I::IntoIter: ExactSizeIterator,
    {
        let mut this = self.project();

        let values = values.into_iter();
        assert!(
            values.len() == this.types.len(),
            "expected {} values but got {}",
            this.types.len(),
            values.len(),
        );

        this.buf.put_i16(this.types.len() as i16);

        for (i, (value, type_)) in values.zip(this.types).enumerate() {
            let idx = this.buf.len();
            this.buf.put_i32(0);
            let len = match value
                .borrow_to_sql()
                .to_sql_checked(type_, this.buf)
                .map_err(|e| Error::to_sql(e, i))?
            {
                IsNull::Yes => -1,
                IsNull::No => i32::try_from(this.buf.len() - idx - 4)
                    .map_err(|e| Error::encode(io::Error::new(io::ErrorKind::InvalidInput, e)))?,
            };
            BigEndian::write_i32(&mut this.buf[idx..], len);
        }

        if this.buf.len() > 4096 {
            this.sink.send(this.buf.split().freeze()).await?;
        }

        Ok(())
    }

    /// Completes the copy, returning the number of rows added.
    ///
    /// This method *must* be used to complete the copy process. If it is not, the copy will be aborted.
    pub async fn finish(self: Pin<&mut Self>) -> Result<u64, Error> {
        let mut this = self.project();

        this.buf.put_i16(-1);
        this.sink.send(this.buf.split().freeze()).await?;
        this.sink.finish().await
    }
}

pub struct StreamReader<S: Stream<Item=Bytes>> {
    stream: S,
}

impl<S: Stream<Item=Bytes>> StreamReader<S> {
    fn new(stream: S) -> Self {
        StreamReader {
            stream
        }
    }
}

impl<S: Stream<Item=Bytes>> AsyncRead for StreamReader<S> {
    fn poll_read(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<std::io::Result<()>> {
        todo!()
    }
}

struct Header {
    has_oids: bool,
}

pin_project! {
    struct BinaryCopyOutContext<R: AsyncReadExt> {
        #[pin]
        reader: R,
        types: Arc<Vec<Type>>,
        header: Option<Header>,
    }
}

pin_project! {
    /// A stream of rows deserialized from the PostgreSQL binary copy format.
    pub struct BinaryCopyOutStream {
        #[pin]
        future: Pin<Box<dyn Future<Output = Option<Result<BinaryCopyOutRow, Error>>>>>,
    }
}

impl BinaryCopyOutStream {
    /// Creates a stream from a raw copy out stream and the types of the columns being returned.
    pub fn new<R: AsyncReadExt + Unpin>(reader: R, types: &[Type]) -> BinaryCopyOutStream {
        let ctx = Box::pin(BinaryCopyOutContext {
            reader,
            types: Arc::new(types.to_vec()),
            header: None,
        });
        BinaryCopyOutStream {
           future: Box::pin(ctx.as_mut().poll_next_option()),
        }
    }
}

impl<R: AsyncReadExt + Unpin> BinaryCopyOutContext<R> {
    async fn poll_next_option(self: Pin<&mut Self>) -> Option<Result<BinaryCopyOutRow, Error>> {
        Some(self.poll_next().await.map_err(Error::parse))
    }

    async fn poll_next(self: Pin<&mut Self>) -> Result<BinaryCopyOutRow, io::Error> {
        let mut this = self.project();

        let has_oids = match &this.header {
            Some(header) => header.has_oids,
            None => {
                let mut magic: &mut [u8] = &mut [0; MAGIC.len()];
                this.reader.read_exact(&mut magic).await?;
                if magic != MAGIC {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "invalid magic value",
                    ));
                }

                let flags = this.reader.read_u32().await?;
                let has_oids = (flags & (1 << 16)) != 0;

                let header_extension_size = this.reader.read_u32().await?;
                // skip header extension
                let mut header_extension: Box<[u8]> = vec![0; header_extension_size as usize].into_boxed_slice();
                this.reader.read_exact(&mut header_extension).await?;

                *this.header = Some(Header { has_oids });
                has_oids
            }
        };

        let mut field_count = this.reader.read_u16().await?;

        if has_oids {
            field_count += 1;
        }
        if field_count as usize != this.types.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("expected {} values but got {}", this.types.len(), field_count),
            ));
        }

        let mut buf = BytesMut::new();
        let mut field_indices = vec![];
        for _ in 0..field_count {
            let field_size = this.reader.read_u32().await?;
            let start = buf.len();
            if field_size == u32::MAX {
                field_indices.push(FieldIndex::Null(start));
                continue;
            }
            let field_size = field_size as usize;
            buf.resize(start + field_size, 0);
            this.reader.read_exact(&mut buf[start..start + field_size]);
            field_indices.push(FieldIndex::Value(start));
        }

        Ok(BinaryCopyOutRow {
            fields: Fields {
                buf,
                indices: field_indices,
            },
            types: this.types.clone(),
        })
    }
}

impl<'a> Stream for BinaryCopyOutStream {
    type Item = Result<BinaryCopyOutRow, Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        this.future.as_mut().poll(cx)
    }
}

enum FieldIndex {
    Value(usize),
    Null(usize),
}

impl FieldIndex {
    fn index(&self) -> usize {
        match self {
            FieldIndex::Value(index) => *index,
            FieldIndex::Null(index) => *index,
        }
    }
}

struct Fields {
    buf: BytesMut,
    indices: Vec<FieldIndex>,
}

impl Fields {
    fn field(&self, idx: usize) -> &[u8] {
        if idx + 1 < self.indices.len() {
            &self.buf[self.indices[idx].index()..self.indices[idx + 1].index()]
        } else {
            &self.buf[self.indices[idx].index()..]
        }
    }
}

/// A row of data parsed from a binary copy out stream.
pub struct BinaryCopyOutRow {
    fields: Fields,
    types: Arc<Vec<Type>>,
}

impl BinaryCopyOutRow {
    /// Like `get`, but returns a `Result` rather than panicking.
    pub fn try_get<'a, T>(&'a self, idx: usize) -> Result<T, Error>
        where
            T: FromSql<'a>,
    {
        let type_ = match self.types.get(idx) {
            Some(type_) => type_,
            None => return Err(Error::column(idx.to_string())),
        };

        if !T::accepts(type_) {
            return Err(Error::from_sql(
                Box::new(WrongType::new::<T>(type_.clone())),
                idx,
            ));
        }

        let r = match &self.fields.indices[idx] {
            FieldIndex::Value(_) => T::from_sql(type_, self.fields.field(idx)),
            FieldIndex::Null(_) => T::from_sql_null(type_),
        };

        r.map_err(|e| Error::from_sql(e, idx))
    }

    /// Deserializes a value from the row.
    ///
    /// # Panics
    ///
    /// Panics if the index is out of bounds or if the value cannot be converted to the specified type.
    pub fn get<'a, T>(&'a self, idx: usize) -> T
        where
            T: FromSql<'a>,
    {
        match self.try_get(idx) {
            Ok(value) => value,
            Err(e) => panic!("error retrieving column {}: {}", idx, e),
        }
    }
}
