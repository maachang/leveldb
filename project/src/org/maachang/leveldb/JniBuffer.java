package org.maachang.leveldb ;

import java.io.IOException;
import java.io.OutputStream;

/**
 * JNI用バッファ.
 */
public class JniBuffer extends OutputStream {
    
    /** クリア時のバッファ保持領域最大長. **/
    protected static final int CLEAR_BY_MAX_BUFFER = 65535 ;
    
    /** バッファ情報. **/
    public long address ;
    public int length ;
    public int position ;
    
    /**
     * コンストラクタ.
     */
    public JniBuffer() {
        address = 0L ;
        length = 0 ;
        position = 0 ;
    }
    
    /**
     * コンストラクタ.
     * @param len メモリー領域を設定します.
     */
    public JniBuffer( int len ) {
        address = 0L ;
        length = 0 ;
        position = 0 ;
        if( len > 0 ) {
            address = jni.malloc( len ) ;
            length = len ;
        }
    }
    
    /**
     * デストラクタ.
     */
    protected void finalize() throws Exception {
        destroy() ;
    }
    
    /**
     * メモリ領域を直接セット.
     * @param addr 対象のアドレスを設定します.
     * @param len 新しい長さを設定します.
     * @param pos 対象のポジションを設定します.
     */
    protected void set( long addr,int len,int pos ) {
        
        // 前回のメモリが存在する場合.
        if( address != 0L ) {
            jni.free( address ) ;
            address = 0L ;
            length = 0 ;
            position = 0 ;
        }
        
        // 対象メモリが存在する場合.
        if( addr != 0L ) {
            address = addr ;
            length = len ;
            position = pos ;
        }
        // 対象メモリが存在しない場合.
        else {
            length = 0 ;
            position = 0 ;
        }
    }
    
    /**
     * バッファ情報を調整.
     * @param copy [true]の場合、バッファを作成した場合に、前の情報をコピーします.
     * @param newLen 新しく作成するサイズを設定します.
     * @return long アドレスが返却されます.
     */
    public long recreate( boolean copy,int newLen ) {
        
        // 現在の長さよりも小さい場合は、生成しない.
        if( length >= newLen ) {
            return address ;
        }
        // 小さすぎる値の場合.
        else if( newLen < 8 ) {
            if( length >= 8 ) {
                return address ;
            }
            newLen = 8 ;
        }
        // 一定以上の場合.
        else {
            
            // newLenの1.5倍のサイズで生成.
            newLen = newLen + ( newLen >> 1 ) ;
        }
        
        // 前回の情報をコピーする場合.
        if( copy ) {
            
            // 前回のメモリ情報が存在する場合.
            if( address != 0L ) {
                
                long t = jni.malloc( newLen ) ;
                jni.memcpy( t,address,length ) ;
                jni.free( address ) ;
                address = t ;
            }
            // メモリが生成されていない場合.
            else {
                
                address = jni.malloc( newLen ) ;
            }
        }
        // 前回の情報をコピーしない場合.
        else {
            
            // 前回のメモリ情報が存在する場合.
            if( address != 0L ) {
                
                jni.free( address ) ;
            }
            address = jni.malloc( newLen ) ;
        }
        length = newLen ;
        return address ;
    }
    
    /**
     * バッファデータを破棄.
     * @return JniBuffer オブジェクトが返却されます.
     */
    public JniBuffer destroy() {
        if( address != 0L ) {
            jni.free( address ) ;
            address = 0L ;
            length = 0 ;
            position = 0 ;
            
            //new Throwable( "trace" ).printStackTrace() ;
        }
        return this ;
    }
    
    /**
     * 情報をクリア.
     * @param len 指定サイズよりも小さいバッファサイズの場合は、対象サイズで生成します.
     * @return JniBuffer オブジェクトが返却されます.
     */
    public JniBuffer clear( int len ) {
        if( address != 0L ) {
            if( length > len ) {
                position = 0 ;
                return this ;
            }
            jni.free( address ) ;
        }
        address = jni.malloc( len ) ;
        length = len ;
        position = 0 ;
        return this ;
    }
    
    /**
     * 情報をクリア.
     * @return JniBuffer オブジェクトが返却されます.
     */
    public JniBuffer clear() {
        return clear( false ) ;
    }
    
    /**
     * 情報をクリア.
     * @param mode [true]の場合は、現在のバッファ長が一定領域を超えている場合は、クリアします.
     * @return JniBuffer オブジェクトが返却されます.
     */
    public JniBuffer clear( boolean mode ) {
        if( address != 0L ) {
            
            // 指定メモリのサイズが規定値を超える場合は、
            // 一旦削除して、規定値のサイズで生成.
            if( mode && length > CLEAR_BY_MAX_BUFFER ) {
                destroy() ;
                address = jni.malloc( CLEAR_BY_MAX_BUFFER ) ;
                length = CLEAR_BY_MAX_BUFFER ;
            }
            else {
                position = 0 ;
            }
        }
        return this ;
    }
    
    /**
     * バッファポインタを取得.
     * @return long バッファポインタが返却されます.
     */
    public long address() {
        return address ;
    }
    
    /**
     * バッファ全体長を取得.
     * @return int バッファ全体長が返却されます.
     */
    public int length() {
        return length ;
    }
    
    /**
     * 現在のポジションを取得.
     * @return int 現在のポジションが返却されます.
     */
    public int position() {
        return position ;
    }
    
    /**
     * 現在のポジションを設定.
     * @param p 対象のポジションを設定します.
     */
    public void position( int p ) {
        recreate( true,p+1 ) ;
        position = p ;
    }
    
    /** 書き込み処理. **/
    private void _write( boolean copy,int b ) {
        recreate( copy,position + 1 ) ;
        JniIO.put( address,position,(byte)b ) ;
        position += 1 ;
    }
    
    /** 書き込み処理. **/
    private void _write( boolean copy,byte b[] ) {
        _write( copy,b,0,b.length );
    }
    
    /** 書き込み処理. **/
    private void _write( boolean copy,byte b[],int off,int len ) {
        if (b == null) {
            throw new NullPointerException();
        } else if ((off < 0) || (off > b.length) || (len < 0) ||
                   ((off + len) > b.length) || ((off + len) < 0)) {
            throw new IndexOutOfBoundsException();
        } else if (len == 0) {
            return;
        }
        recreate( copy,position + len + 1 ) ;
        jni.putBinary( address+position,b,off,len ) ;
        position += len ;
    }
    
    /**
     * クローズ処理.
     * @exception IOException 例外.
     */
    public void close() throws IOException {
        clear() ;
    }
    
    /**
     * 更新処理.
     * ※jniBufferでは意味がありません.
     * @exception IOException 例外.
     */
    public void flush() throws IOException {
    }
    
    /**
     * 書き込み処理.
     * @param b 対象の情報を設定します.
     * @exception IOException 例外.
     */
    public void write(int b) throws IOException {
        _write( true,b ) ;
    }
    
    /**
     * 書き込み処理.
     * @param b 対象の情報を設定します.
     * @exception IOException 例外.
     */
    public void write(byte b[]) throws IOException {
        _write( true,b,0,b.length );
    }
    
    /**
     * 書き込み処理.
     * @param b 対象の情報を設定します.
     * @param off 対象のオフセット値を設定します.
     * @param len 対象の長さを設定します.
     * @exception IOException 例外.
     */
    public void write(byte b[], int off, int len) throws IOException {
        _write( true,b,off,len );
    }
    
    /**
     * 1バイトの情報を設定.
     * @param value 対象の１バイト情報を設定します.
     * @return JniBuffer オブジェクトが返却されます.
     */
    public JniBuffer set( int value ) {
        position = 0 ;
        _write( false,value ) ;
        return this ;
    }
    
    /**
     * 1バイトの情報を設定.
     * @param value 対象の１バイト情報を設定します.
     * @return JniBuffer オブジェクトが返却されます.
     */
    public JniBuffer set( byte value ) {
        return set( (int)value ) ;
    }
    
    /**
     * 1バイトの情報を取得.
     * @return Byte バイト情報が返されます.
     */
    public Byte get() {
        if( position == 0 ) {
            return null ;
        }
        return JniIO.get( address,0 ) ;
    }
    
    /**
     * binary情報を設定.
     * @param value 設定対象の情報を設定します.
     * @return JniBuffer オブジェクトが返却されます.
     */
    public JniBuffer setBinary( byte[] value ) {
        return setBinary( value,0,value.length ) ;
    }
    
    /**
     * binary情報を設定.
     * @param value 設定対象の情報を設定します.
     * @param len 対象のデータ長を設定します.
     * @return JniBuffer オブジェクトが返却されます.
     */
    public JniBuffer setBinary( byte[] value,int len ) {
        return setBinary( value,0,len ) ;
    }
    
    /**
     * binary情報を設定.
     * @param value 設定対象の情報を設定します.
     * @param off 対象のオフセット値を設定します.
     * @param len 対象のデータ長を設定します.
     * @return JniBuffer オブジェクトが返却されます.
     */
    public JniBuffer setBinary( byte[] value,int off,int len ) {
        position = 0 ;
        _write( false,value,off,len ) ;
        return this ;
    }
    
    /**
     * binary情報を取得.
     * @param value 取得対象の情報を設定します.
     * @return int 取得された長さが返されます.
     */
    public byte[] getBinary() {
        if( position == 0 ) {
            return null ;
        }
        byte[] ret = new byte[ position ] ;
        jni.getBinary( address,ret,0,position ) ;
        return ret ;
    }
    
    /**
     * binary情報を取得.
     * @param value 取得対象の情報を設定します.
     * @return int 取得された長さが返されます.
     */
    public int getBinary( byte[] value ) {
        return getBinary( value,0,value.length ) ;
    }
    
    /**
     * binary情報を取得.
     * @param value 取得対象の情報を設定します.
     * @param length 対象のデータ長を設定します.
     * @return int 取得された長さが返されます.
     */
    public int getBinary( byte[] value,int len ) {
        return getBinary( value,0,len ) ;
    }
    
    /**
     * binary情報を取得.
     * @param value 取得対象の情報を設定します.
     * @param off 対象のオフセット値を設定します.
     * @param length 対象のデータ長を設定します.
     * @return int 取得された長さが返されます.
     */
    public int getBinary( byte[] value,int off,int len ) {
        if( position < off + len ) {
            len = position - off ;
        }
        if( value.length < len ) {
            len = value.length ;
        }
        if( len == 0 || position == 0 ) {
            return -1 ;
        }
        jni.getBinary( address,value,off,len ) ;
        return len ;
    }
    
    /**
     * 文字情報を新規セット.
     * @param string 対象の文字列を設定します.
     * @return JniBuffer オブジェクトが返却されます.
     * @exception Exception 例外.
     */
    public JniBuffer setString( String string )
        throws Exception {
        position = 0 ;
        setString( string,0,string.length() ) ;
        return this ;
    }
    
    /**
     * 文字情報を新規セット.
     * @param string 対象の文字列を設定します.
     * @param off 対象のオフセットを設定します.
     * @param len 対象の長さを設定します.
     * @return JniBuffer オブジェクトが返却されます.
     * @exception Exception 例外.
     */
    public JniBuffer setString( String string,int off,int len )
        throws Exception {
        if (string == null) {
            throw new NullPointerException();
        } else if ((off < 0) || (off > string.length()) || (len < 0) ||
                   ((off + len) > string.length()) || ((off + len) < 0)) {
            throw new IndexOutOfBoundsException();
        } else if (len == 0) {
            return this ;
        }
        position = 0 ;
        recreate( false,( len * 3 ) + 1 ) ;
        int res = JniIO.putUtf16( address,0,string,off,len ) ;
        position += res ;
        return this ;
    }
    
    /**
     * 文字情報を取得.
     * @return String 文字情報が返却されます.
     * @exception Exception 例外.
     */
    public String getString() throws Exception {
        return getString( position ) ;
    }
    
    /**
     * 文字情報を取得.
     * @param len 対象のバイナリ長を設定します.
     * @return String 文字情報が返却されます.
     * @exception Exception 例外.
     */
    public String getString( int len )
        throws Exception {
        if( len > position ) {
            len = position ;
        }
        if( len == 0 || position == 0 ) {
            return null ;
        }
        return JniIO.getUtf16( address,0,len ) ;
    }
    
    /**
     * boolean設定.
     * @param value 設定対象の情報を設定します.
     * @return JniBuffer オブジェクトが返却されます.
     */
    public JniBuffer setBoolean( boolean value ) {
        position = 0 ;
        recreate( false,1 ) ;
        JniIO.put( address,0,(byte)( (value)?1:0 ) ) ;
        position = 1 ;
        return this ;
    }
    
    /**
     * boolean取得.
     * @param address 対象のアドレスを設定します.
     * @param index 対象のインデックス位置を設定します.
     * @return boolean 情報が返されます.
     */
    public Boolean getBoolean() {
        if( position == 0 ) {
            return null ;
        }
        return JniIO.get( address,0 ) == 1 ;
    }
    
    /**
     * char設定.
     * @param value 設定対象の情報を設定します.
     * @return JniBuffer オブジェクトが返却されます.
     */
    public JniBuffer setChar( char value ) {
        position = 0 ;
        recreate( false,2 ) ;
        JniIO.putChar( address,0,value ) ;
        position = 2 ;
        return this ;
    }
    
    /**
     * char取得.
     * @return Character 情報が返されます.
     */
    public Character getChar() {
        if( position < 2 ) {
            return null ;
        }
        return JniIO.getChar( address,0 ) ;
    }
    
    /**
     * char取得.
     * <p>Endian変換を行います.</p>
     * @return Character 情報が返されます.
     */
    public Character getCharE() {
        if( position < 2 ) {
            return null ;
        }
        return JniIO.getCharE( address,0 ) ;
    }
    
    /**
     * short設定.
     * @param value 設定対象の情報を設定します.
     * @return JniBuffer オブジェクトが返却されます.
     */
    public JniBuffer setShort( short value ) {
        position = 0 ;
        recreate( false,2 ) ;
        JniIO.putShort( address,0,value ) ;
        position = 2 ;
        return this ;
    }
    
    /**
     * short取得.
     * @return Short 情報が返されます.
     */
    public Short getShort() {
        if( position < 2 ) {
            return null ;
        }
        return JniIO.getShort( address,0 ) ;
    }
    
    /**
     * short取得.
     * <p>Endian変換を行います.</p>
     * @return Short 情報が返されます.
     */
    public Short getShortE() {
        if( position < 2 ) {
            return null ;
        }
        return JniIO.getShortE( address,0 ) ;
    }
    
    /**
     * int設定.
     * @param value 設定対象の情報を設定します.
     * @return JniBuffer オブジェクトが返却されます.
     */
    public JniBuffer setInt( int value ) {
        position = 0 ;
        recreate( false,4 ) ;
        JniIO.putInt( address,0,value ) ;
        position = 4 ;
        return this ;
    }
    
    /**
     * int取得.
     * @return Integer 情報が返されます.
     */
    public Integer getInt() {
        if( position < 4 ) {
            return null ;
        }
        return JniIO.getInt( address,0 ) ;
    }
    
    /**
     * int取得.
     * <p>Endian変換を行います.</p>
     * @return Integer 情報が返されます.
     */
    public Integer getIntE() {
        if( position < 4 ) {
            return null ;
        }
        return JniIO.getIntE( address,0 ) ;
    }
    
    /**
     * long設定.
     * @param value 設定対象の情報を設定します.
     * @return JniBuffer オブジェクトが返却されます.
     */
    public JniBuffer setLong( long value ) {
        position = 0 ;
        recreate( false,8 ) ;
        JniIO.putLong( address,0,value ) ;
        position = 8 ;
        return this ;
    }
    
    /**
     * long取得.
     * @return Long 情報が返されます.
     */
    public Long getLong() {
        if( position < 8 ) {
            return null ;
        }
        return JniIO.getLong( address,0 ) ;
    }
    
    /**
     * long取得.
     * <p>Endian変換を行います.</p>
     * @return Long 情報が返されます.
     */
    public Long getLongE() {
        if( position < 8 ) {
            return null ;
        }
        return JniIO.getLongE( address,0 ) ;
    }
    
    /**
     * float設定.
     * @param value 設定対象の情報を設定します.
     * @return JniBuffer オブジェクトが返却されます.
     */
    public JniBuffer setFloat( float value ) {
        position = 0 ;
        recreate( false,4 ) ;
        JniIO.putFloat( address,0,value ) ;
        position = 4 ;
        return this ;
    }
    
    /**
     * float取得.
     * @return Float 情報が返されます.
     */
    public Float getFloat() {
        if( position < 4 ) {
            return null ;
        }
        return JniIO.getFloat( address,0 ) ;
    }
    
    /**
     * float取得.
     * <p>Endian変換を行います.</p>
     * @return Float 情報が返されます.
     */
    public Float getFloatE() {
        if( position < 4 ) {
            return null ;
        }
        return JniIO.getFloatE( address,0 ) ;
    }
    
    /**
     * double設定.
     * @param value 設定対象の情報を設定します.
     * @return JniBuffer オブジェクトが返却されます.
     */
    public JniBuffer setDouble( double value ) {
        position = 0 ;
        recreate( false,8 ) ;
        JniIO.putDouble( address,0,value ) ;
        position = 8 ;
        return this ;
    }
    
    /**
     * double取得.
     * @return Double 情報が返されます.
     */
    public Double getDouble() {
        if( position < 8 ) {
            return null ;
        }
        return JniIO.getDouble( address,0 ) ;
    }
    
    /**
     * double取得.
     * <p>Endian変換を行います.</p>
     * @return Double 情報が返されます.
     */
    public Double getDoubleE() {
        if( position < 8 ) {
            return null ;
        }
        return JniIO.getDoubleE( address,0 ) ;
    }
    
    /**
     * C言語用文字情報を新規セット.
     * ※この処理では、OSデフォルトの文字コードで、変換され、
     *   最終部分の文字に\0が格納されます.
     * @param string 対象の文字列を設定します.
     * @return JniBuffer オブジェクトが返却されます.
     */
    protected JniBuffer setJniChar( String string ) {
        position = 0 ;
        _write( false,string.getBytes() ) ;
        _write( true,0 ) ;
        return this ;
    }
    
    /**
     * C言語用文字情報を新規セット.
     * ※この処理では、OSデフォルトの文字コードで、変換され、
     *   最終部分の文字に\0が格納されます.
     * @param string 対象の文字列を設定します.
     * @param off 対象のオフセットを設定します.
     * @param len 対象の長さを設定します.
     * @return JniBuffer オブジェクトが返却されます.
     */
    protected JniBuffer setJniChar( String string,int off,int len ) {
        position = 0 ;
        _write( false,string.substring( off,off+len ).getBytes() ) ;
        _write( true,0 ) ;
        return this ;
    }
    
    /**
     * オブジェクトの状態を文字列に出力.
     * @return String 文字列が返却されます.
     */
    public String toString() {
        return new StringBuilder( "length:" ).
            append( length ).append( " position:" ).
            append( position ).toString() ;
    }
    
}
