//! Utilities for working with the PostgreSQL binary copy format.

use core::result;
use futures_util::future::{LocalBoxFuture};
use std::borrow::BorrowMut;
use std::cell::{RefCell};
use std::convert::TryFrom;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll};

use byteorder::{BigEndian, ByteOrder};
use bytes::{BufMut, Bytes, BytesMut};
use futures_util::{FutureExt, SinkExt, Stream};
use pin_project::pin_project;
use postgres_types::{BorrowToSql};
use tokio::io;
use tokio::io::{AsyncRead, AsyncReadExt};

use crate::types::{FromSql, IsNull, ToSql, Type, WrongType};
use crate::{slice_iter, CopyInSink, Error};

const MAGIC: &[u8] = b"PGCOPY\n\xff\r\n\0";

/// A type which serializes rows into the PostgreSQL binary copy format.
///
/// The copy *must* be explicitly completed via the `finish` method. If it is not, the copy will be aborted.
#[pin_project]
pub struct BinaryCopyInWriter {
    #[pin]
    sink: CopyInSink<Bytes>,
    types: Vec<Type>,
    buf: BytesMut,
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
    pub async fn write(self: Pin<&mut Self>, values: &[&(dyn ToSql + Sync)]) -> Result<()> {
        self.write_raw(slice_iter(values)).await
    }

    /// A maximally-flexible version of `write`.
    ///
    /// # Panics
    ///
    /// Panics if the number of values provided does not match the number expected.
    pub async fn write_raw<P, I>(self: Pin<&mut Self>, values: I) -> Result<()>
    where
        P: BorrowToSql,
        I: IntoIterator<Item = P>,
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
    pub async fn finish(self: Pin<&mut Self>) -> Result<u64> {
        let mut this = self.project();

        this.buf.put_i16(-1);
        this.sink.send(this.buf.split().freeze()).await?;
        this.sink.finish().await
    }
}

#[derive(Copy, Clone)]
struct Header {
    has_oids: bool,
}

type Result<T> = result::Result<T, Error>;

/// A type which deserializes rows from the PostgreSQL binary copy format.
pub struct BinaryCopyOutStream<'f, 'r, R>
where
    R: AsyncReadExt + Unpin + 'r,
    'r: 'f,
{
    _r: PhantomData<&'r ()>,
    reader: Rc<RefCell<R>>,
    types: Rc<Vec<Type>>,
    header: Rc<RefCell<Option<Header>>>,
    future: Option<LocalBoxFuture<'f, Result<BinaryCopyOutRow>>>,
}

impl<'f, 'r, R> BinaryCopyOutStream<'f, 'r, R>
where
    R: AsyncReadExt + Unpin + 'r,
{
    /// Creates a stream from a raw copy out stream and the types of the columns being returned.
    pub fn new(reader: R, types: &[Type]) -> BinaryCopyOutStream<'f, 'r, R> {
        BinaryCopyOutStream {
            _r: PhantomData,
            reader: Rc::new(RefCell::new(reader)),
            types: Rc::new(types.to_vec()),
            header: Rc::new(RefCell::new(None)),
            future: None,
        }
    }
}

impl<'f, 'r, R> Stream for BinaryCopyOutStream<'f, 'r, R>
where
    R: AsyncRead + Unpin + 'r,
{
    type Item = Result<BinaryCopyOutRow>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self;

        if this.future.is_none() {
            let future =
                poll_next_row(this.reader.clone(), this.types.clone(), this.header.clone())
                    .boxed_local();
            this.future = Some(future);
        }

        match Pin::new(&mut this.future.borrow_mut().as_mut().unwrap()).poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Ok(row)) => {
                this.future.take();
                Poll::Ready(Some(Ok(row)))
            }
            Poll::Ready(Err(_)) => {
                this.future.take();
                Poll::Ready(None) // TODO
            }
        }
    }
}
async fn poll_next_row<R>(
    reader: Rc<RefCell<R>>,
    types: Rc<Vec<Type>>,
    header: Rc<RefCell<Option<Header>>>,
) -> Result<BinaryCopyOutRow>
where
    R: AsyncRead + Unpin,
{
    _poll_next_row(reader, types, header)
        .await
        .map_err(Error::parse)
}

async fn _poll_next_row<R>(
    reader: Rc<RefCell<R>>,
    types: Rc<Vec<Type>>,
    header: Rc<RefCell<Option<Header>>>,
) -> io::Result<BinaryCopyOutRow>
where
    R: AsyncRead + Unpin,
{
    let mut reader = reader.as_ref().borrow_mut();

    let has_oids = if header.borrow().is_none() {
        let mut magic: &mut [u8] = &mut [0; MAGIC.len()];
        reader.read_exact(&mut magic).await?;
        println!("magic: {:?}", magic);
        if magic != MAGIC {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid magic value",
            ));
        }

        let flags = reader.read_u32().await?;
        println!("flags: {:?}", flags);
        let has_oids = (flags & (1 << 16)) != 0;

        let header_extension_size = reader.read_u32().await?;
        println!("header extension size: {:?}", header_extension_size);

        // skip header extension
        let mut header_extension: Box<[u8]> =
            vec![0; header_extension_size as usize].into_boxed_slice();
        reader.read_exact(&mut header_extension).await?;
        println!("header extension: {:?}", header_extension);

        header.replace(Some(Header { has_oids }));
        has_oids
    } else {
        header.borrow().unwrap().has_oids
    };

    let mut field_count = reader.read_u16().await?;
    println!("field count: {:?}", field_count);

    if has_oids {
        field_count += 1;
    }
    if field_count as usize != types.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("expected {} values but got {}", types.len(), field_count),
        ));
    }

    let mut field_buf = BytesMut::new();
    let mut field_indices = vec![];
    for _ in 0..field_count {
        let field_size = reader.read_u32().await?;
        println!("field size: {:?}", field_size);

        let start = field_buf.len();
        if field_size == u32::MAX {
            field_indices.push(FieldIndex::Null(start));
            continue;
        }
        let field_size = field_size as usize;
        field_buf.resize(start + field_size, 0);
        reader.read_exact(&mut field_buf[start..start + field_size]).await?;
        println!("buf: {:?}", field_buf);
        field_indices.push(FieldIndex::Value(start));
    }

    Ok(BinaryCopyOutRow {
        fields: Fields {
            buf: field_buf,
            indices: field_indices,
        },
        types: types.clone(),
    })
}

#[derive(Debug)]
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

#[derive(Debug)]
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
#[derive(Debug)]
pub struct BinaryCopyOutRow {
    fields: Fields,
    types: Rc<Vec<Type>>,
}

impl BinaryCopyOutRow {
    /// Like `get`, but returns a `Result` rather than panicking.
    pub fn try_get<'a, T>(&'a self, idx: usize) -> Result<T>
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
