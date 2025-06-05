using EasyCompressor;

namespace UltraKV
{
    /// <summary>
    /// 压缩解压函数
    /// 推荐算法：LZ4/Zstd/Snappy
    /// 支持算法：LZ4/Zstd/Snappy/LZMA/Deflate/Brotli
    /// </summary>
    public static class CompressionHelper
    {
        /// <summary>
        /// 使用指定的算法压缩数据，并在需要时进行加密
        /// </summary>
        /// <param name="buffer">要压缩的字节数组</param>
        /// <param name="compressionType">压缩算法类型（"LZ4"、"Zstd"或"Snappy"）</param>
        /// <returns>压缩后的字节数组</returns>
        public static byte[] Compress(byte[] buffer, string compressionType)
        {
            // 压缩数据
            buffer = compressionType switch
            {
                "LZ4" => LZ4Compressor.Shared.Compress(buffer),
                "Zstd" => ZstdSharpCompressor.Shared.Compress(buffer),
                "Snappy" => SnappierCompressor.Shared.Compress(buffer),
                "LZMA" => LZMACompressor.Shared.Compress(buffer),
                "Deflate" => DeflateCompressor.Shared.Compress(buffer),
                "Brotli" => BrotliCompressor.Shared.Compress(buffer),
                _ => buffer
            };

            return buffer;
        }

        /// <summary>
        /// 使用指定的算法解压数据，并在需要时进行解密
        /// </summary>
        /// <param name="buffer">要解压的字节数组</param>
        /// <param name="compressionType">压缩算法类型（"LZ4"、"Zstd"或"Snappy"）</param>
        /// <returns>解压后的字节数组</returns>
        public static byte[] Decompress(byte[] buffer, string compressionType)
        {
            // 解压数据
            buffer = compressionType switch
            {
                "LZ4" => LZ4Compressor.Shared.Decompress(buffer),
                "Zstd" => ZstdSharpCompressor.Shared.Decompress(buffer),
                "Snappy" => SnappierCompressor.Shared.Decompress(buffer),
                "LZMA" => LZMACompressor.Shared.Decompress(buffer),
                "Deflate" => DeflateCompressor.Shared.Decompress(buffer),
                "Brotli" => BrotliCompressor.Shared.Decompress(buffer),
                _ => buffer
            };

            return buffer;
        }
    }
}