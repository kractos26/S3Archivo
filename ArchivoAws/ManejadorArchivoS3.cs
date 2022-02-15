using System;
using System.Collections.Generic;
using System.Configuration;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Mime;
using System.Text;
using System.Threading.Tasks;
using Amazon;
using Amazon.S3;
using Amazon.S3.Encryption;
using Amazon.S3.Model;
using Microsoft.Azure.Management.Compute.Fluent.Models;

namespace ArchivoAws
{
    public class ManejadorArchivoS3
    {
        private static IAmazonS3 client;
        public ManejadorArchivoS3()
        {
            string regionconf = ConfigurationManager.AppSettings["regionname"];
            RegionEndpoint region = RegionEndpoint.GetBySystemName(regionconf);
            client = new AmazonS3Client(region);
        }
        /// <summary>
        /// Permite la carga de Ficheros a S3 de forma asincronica.
        /// </summary>
        /// <param name="objectFile">contiene los datos del fichero</param>
        /// <returns></returns>
        public async Task<ManejadorArchivoS3Response<bool>> CargarArchivoAsync(ObjectFile objectFile)
        {
            ManejadorArchivoS3Response<bool> manejadorArchivoS3Response = new ManejadorArchivoS3Response<bool>();
            try
            {
                PutObjectRequest putrespuesta = new PutObjectRequest
                {
                    BucketName = objectFile.Contenedor,
                    Key = string.Format($"{objectFile.Prefijo}/{objectFile.Clave}"),
                    ContentType = objectFile.TipoContenido,
                    InputStream = objectFile.ContenidoArchivo,
                };
                objectFile.MetaDatos.ToList().ForEach(itemmeta =>
                {
                    putrespuesta.Metadata.Add(itemmeta.Key, itemmeta.Value);
                });

                PutObjectResponse putObjectResponse = await client.PutObjectAsync(putrespuesta);
                manejadorArchivoS3Response.CodigoEstado = putObjectResponse.HttpStatusCode;
                manejadorArchivoS3Response.EntidaRespuesta = true;
            }
            catch (AmazonS3Exception e)
            {
                manejadorArchivoS3Response.MensajeError = e.Message;
                manejadorArchivoS3Response.CodigoEstado = e.StatusCode;
                manejadorArchivoS3Response.EntidaRespuesta = false;
                return manejadorArchivoS3Response;
            }
            return manejadorArchivoS3Response;

        }

        /// <summary>
        /// Permite obtener archivos de un bucket en S3.
        /// </summary>
        /// <param name="clave"> Identificador del archivo.</param>
        /// <param name="Contenedor">Bucket donde se busca el archivo.</param>
        /// <returns></returns>
        public async Task<ManejadorArchivoS3Response<ObjectFile>> ObtenerArchivoAsync(string clave, string prefijo, string Contenedor, TypeMetadato? typeMetadato = null)
        {
            ManejadorArchivoS3Response<ObjectFile> manejadorArchivoS3Response = new ManejadorArchivoS3Response<ObjectFile>();
            manejadorArchivoS3Response.EntidaRespuesta = new ObjectFile();
            try
            {
                GetObjectRequest getObjectRequest = new GetObjectRequest()
                {
                    BucketName = Contenedor,
                    Key = string.Format($"{prefijo}/{clave}")

                };

                GetObjectResponse getObjectResponse = await client.GetObjectAsync(getObjectRequest);

                manejadorArchivoS3Response.EntidaRespuesta.Contenedor = getObjectResponse.BucketName;
                manejadorArchivoS3Response.EntidaRespuesta.Clave = clave;
                manejadorArchivoS3Response.EntidaRespuesta.TipoContenido = getObjectResponse.Headers["Content-Type"];
                manejadorArchivoS3Response.EntidaRespuesta.ContenidoArchivo = getObjectResponse.ResponseStream;
                manejadorArchivoS3Response.CodigoEstado = getObjectResponse.HttpStatusCode;
                if (typeMetadato == null)
                {
                    return manejadorArchivoS3Response;
                }
                else
                {
                    return getObjectResponse.Metadata.Keys.Contains(typeMetadato.ToString()) ? manejadorArchivoS3Response : new ManejadorArchivoS3Response<ObjectFile>();
                }
            }
            catch (AmazonS3Exception e)
            {
                manejadorArchivoS3Response.MensajeError = e.Message;
                manejadorArchivoS3Response.CodigoEstado = e.StatusCode;
                return manejadorArchivoS3Response;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="clave"></param>
        /// <param name="BucketName"></param>
        /// <returns></returns>
        async Task<ManejadorArchivoS3Response<bool>> EliminarFicheroAsyc(string clave, string BucketName)
        {
            ManejadorArchivoS3Response<bool> manejadorArchivoS3Response = new ManejadorArchivoS3Response<bool>();
            try
            {
                DeleteObjectRequest deleteObjectRequest = new DeleteObjectRequest
                {
                    BucketName = clave,
                    Key = BucketName
                };

                DeleteObjectResponse resultdelete = await client.DeleteObjectAsync(deleteObjectRequest);
                manejadorArchivoS3Response.CodigoEstado = resultdelete.HttpStatusCode;
                manejadorArchivoS3Response.EntidaRespuesta = true;
            }
            catch (AmazonS3Exception e)
            {
                manejadorArchivoS3Response.EntidaRespuesta = true;
                manejadorArchivoS3Response.CodigoEstado = e.StatusCode;
                return manejadorArchivoS3Response;
            }
            return manejadorArchivoS3Response;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="duration"></param>
        /// <param name="objectFile"></param>
        /// <returns></returns>
        string GenerarPreUrl(double duration, ObjectFile objectFile)
        {
            string urlString = string.Empty;
            try
            {

                GetPreSignedUrlRequest request = new GetPreSignedUrlRequest
                {
                    BucketName = objectFile.Contenedor,
                    Key = objectFile.Clave,
                    Expires = DateTime.UtcNow.AddHours(duration)
                };
                urlString = client.GetPreSignedURL(request);
            }
            catch (AmazonS3Exception e)
            {
                throw;
            }
            return urlString;
        }

        /// <summary>
        /// Permite crear un contenedor de objetos.
        /// </summary>
        /// <param name="Contenedor">nombre del contenedor.</param>
        /// <returns></returns>
        public async Task<ManejadorArchivoS3Response<bool>> CrearContenedorAsync(string Contenedor)
        {
            ManejadorArchivoS3Response<bool> manejadorArchivoS3Response = new ManejadorArchivoS3Response<bool>();
            try
            {
                if (!(await client.DoesS3BucketExistAsync(Contenedor)))
                {
                    PutBucketRequest putcontenedorrequest = new PutBucketRequest
                    {
                        BucketName = Contenedor,
                        UseClientRegion = true
                    };
                    PutBucketResponse putBucketResponse = await client.PutBucketAsync(putcontenedorrequest);
                    manejadorArchivoS3Response.CodigoEstado = putBucketResponse.HttpStatusCode;
                    manejadorArchivoS3Response.EntidaRespuesta = true;
                }
                else
                {
                    manejadorArchivoS3Response.CodigoEstado = HttpStatusCode.Conflict;
                    manejadorArchivoS3Response.MensajeError = "El contenedor se encuentra creado";
                    manejadorArchivoS3Response.EntidaRespuesta = false;
                }
            }
            catch (AmazonS3Exception e)
            {
                manejadorArchivoS3Response.CodigoEstado = e.StatusCode;
                manejadorArchivoS3Response.MensajeError = e.Message;
                return manejadorArchivoS3Response;
            }
            return manejadorArchivoS3Response;
        }
        /// <summary>
        /// Permite copiar un objeto file a otro contenedor.
        /// </summary>
        /// <param name="contenedorOrigen"></param>
        /// <param name="contenorDestino"></param>
        /// <param name="claveOrigen"></param>
        /// <param name="ClaveDestino"></param>
        /// <returns></returns>
        async Task<ManejadorArchivoS3Response<bool>> CambiarFileContenedorAsync(string contenedorOrigen, string contenorDestino, string claveOrigen, string ClaveDestino)
        {
            ManejadorArchivoS3Response<bool> manejadorArchivoS3Response = new ManejadorArchivoS3Response<bool>();
            try
            {
                CopyObjectRequest request = new CopyObjectRequest
                {
                    SourceBucket = contenedorOrigen,
                    SourceKey = claveOrigen,
                    DestinationBucket = contenorDestino,
                    DestinationKey = ClaveDestino
                };
                CopyObjectResponse response = await client.CopyObjectAsync(request);
                manejadorArchivoS3Response.EntidaRespuesta = true;
                manejadorArchivoS3Response.CodigoEstado = response.HttpStatusCode;
            }
            catch (AmazonS3Exception e)
            {
                manejadorArchivoS3Response.CodigoEstado = e.StatusCode;
                manejadorArchivoS3Response.MensajeError = e.Message;
                return manejadorArchivoS3Response;
            }
            return manejadorArchivoS3Response;
        }

        /// <summary>
        /// Obtiene la lista archivos de un contenedor.
        /// </summary>
        /// <param name="contenedor">Nombre del contenedor.</param>
        /// <param name="prefijo">Parte inicial de la clave que clasifica el grupo de archivos.</param>
        /// <returns></returns>
        async Task<ManejadorArchivoS3Response<List<ObjectFile>>> ListaArchivoAsync(string contenedor, string prefijo, TypeMetadato tipometadato)
        {
            ManejadorArchivoS3Response<List<ObjectFile>> manejadorArchivoS3Response = new ManejadorArchivoS3Response<List<ObjectFile>>();

            try
            {

                ListObjectsResponse response = await client.ListObjectsAsync(contenedor, prefijo);
                if (response.IsTruncated)
                {
                    if (response.S3Objects.Any())
                    {
                        response.S3Objects.ForEach(item =>
                        {
                            Task<ManejadorArchivoS3Response<ObjectFile>> objeto = ObtenerArchivoAsync(item.Key, prefijo, item.BucketName, tipometadato);
                            if (objeto.Result.EntidaRespuesta.ContenidoArchivo.Length > 0) manejadorArchivoS3Response.EntidaRespuesta.Add(objeto.Result.EntidaRespuesta);
                        });
                    }
                }
                manejadorArchivoS3Response.CodigoEstado = response.HttpStatusCode;

            }
            catch (AmazonS3Exception e)
            {
                manejadorArchivoS3Response.CodigoEstado = e.StatusCode;
                manejadorArchivoS3Response.MensajeError = e.Message;
            }
            return manejadorArchivoS3Response;
        }

        ManejadorArchivoS3Response<bool> CargarArchivo(ObjectFile objectFile)
        {
            ManejadorArchivoS3Response<bool> manejadorArchivoS3Response = new ManejadorArchivoS3Response<bool>();
            PutObjectResponse putObjectResponse = new PutObjectResponse();
            try
            {
                PutObjectRequest putrespuesta = new PutObjectRequest
                {
                    BucketName = objectFile.Contenedor,
                    Key = string.Format($"{objectFile.Prefijo}/{objectFile.Clave}"),
                    ContentType = objectFile.TipoContenido,
                    InputStream = objectFile.ContenidoArchivo,
                };
                objectFile.MetaDatos.ToList().ForEach(itemmeta =>
                {
                    putrespuesta.Metadata.Add(itemmeta.Key, itemmeta.Value);
                });

                putObjectResponse = client.PutObject(putrespuesta);
                manejadorArchivoS3Response.CodigoEstado = putObjectResponse.HttpStatusCode;
                manejadorArchivoS3Response.EntidaRespuesta = true;
            }
            catch (AmazonS3Exception e)
            {
                manejadorArchivoS3Response.MensajeError = e.Message;
                manejadorArchivoS3Response.CodigoEstado = e.StatusCode;
            }
            return manejadorArchivoS3Response;
        }

        /// <summary>
        /// Permite obtener archivos de un bucket en S3.
        /// </summary>
        /// <param name="clave"> Identificador del archivo.</param>
        /// <param name="Contenedor">Bucket donde se busca el archivo.</param>
        /// <returns></returns>
        ManejadorArchivoS3Response<ObjectFile> ObtenerArchivo(string clave, string prefijo, string Contenedor, TypeMetadato? typeMetadato = null)
        {
            ManejadorArchivoS3Response<ObjectFile> objectFile = new ManejadorArchivoS3Response<ObjectFile>();
            try
            {
                GetObjectRequest getObjectRequest = new GetObjectRequest()
                {
                    BucketName = Contenedor,
                    Key = string.Format($"{prefijo}/{clave}"),
                    
                };

                GetObjectResponse getObjectResponse = client.GetObject(getObjectRequest);
                objectFile.EntidaRespuesta.Contenedor = getObjectResponse.BucketName;
                objectFile.EntidaRespuesta.Clave = clave;
                objectFile.EntidaRespuesta.TipoContenido = getObjectResponse.Headers["Content-Type"];
                objectFile.EntidaRespuesta.ContenidoArchivo = getObjectResponse.ResponseStream;
                objectFile.CodigoEstado = getObjectResponse.HttpStatusCode;
                if (typeMetadato == null)
                {
                    return objectFile;
                }
                else
                {
                    return getObjectResponse.Metadata.Keys.Contains(typeMetadato.ToString()) ? objectFile : new ManejadorArchivoS3Response<ObjectFile>();
                }
            }
            catch (AmazonS3Exception e)
            {
                objectFile.MensajeError = e.Message;
                objectFile.CodigoEstado = e.StatusCode;
            }
            return objectFile;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="clave"></param>
        /// <param name="BucketName"></param>
        /// <returns></returns>
        ManejadorArchivoS3Response<bool> EliminarFichero(string clave, string BucketName)
        {
            ManejadorArchivoS3Response<bool> manejadorArchivoS3Response = new ManejadorArchivoS3Response<bool>();
            try
            {
                DeleteObjectRequest deleteObjectRequest = new DeleteObjectRequest
                {
                    BucketName = clave,
                    Key = BucketName
                };

                DeleteObjectResponse deletresponse = client.DeleteObject(deleteObjectRequest);
                manejadorArchivoS3Response.CodigoEstado = deletresponse.HttpStatusCode;
                manejadorArchivoS3Response.EntidaRespuesta = true;
            }
            catch (AmazonS3Exception e)
            {
                manejadorArchivoS3Response.MensajeError = e.Message;
                manejadorArchivoS3Response.CodigoEstado = e.StatusCode;
                return manejadorArchivoS3Response;
            }
            return manejadorArchivoS3Response;
        }
        /// <summary>
        /// 
        /// </summary>
        /// <param name="contenedorOrigen"></param>
        /// <param name="contenorDestino"></param>
        /// <param name="claveOrigen"></param>
        /// <param name="ClaveDestino"></param>
        /// <returns></returns>
        ManejadorArchivoS3Response<bool> CambiarFileContenedor(string contenedorOrigen, string contenorDestino, string claveOrigen, string ClaveDestino)
        {
            ManejadorArchivoS3Response<bool> manejadorArchivoS3Response = new ManejadorArchivoS3Response<bool>();
            CopyObjectResponse response = new CopyObjectResponse();
            try
            {
                CopyObjectRequest request = new CopyObjectRequest
                {
                    SourceBucket = contenedorOrigen,
                    SourceKey = claveOrigen,
                    DestinationBucket = contenorDestino,
                    DestinationKey = ClaveDestino
                };
                response = client.CopyObject(request);
                manejadorArchivoS3Response.CodigoEstado = response.HttpStatusCode;
                manejadorArchivoS3Response.EntidaRespuesta = true;
            }
            catch (AmazonS3Exception e)
            {
                manejadorArchivoS3Response.MensajeError = e.Message;
                manejadorArchivoS3Response.CodigoEstado = e.StatusCode;
                return manejadorArchivoS3Response;
            }
            return manejadorArchivoS3Response;
        }

        /// <summary>
        /// Obtiene la lista archivos de un contenedor.
        /// </summary>
        /// <param name="contenedor">Nombre del contenedor.</param>
        /// <param name="prefijo">Parte inicial de la clave que clasifica el grupo de archivos.</param>
        /// <returns></returns>
        ManejadorArchivoS3Response<List<ObjectFile>> ListaArchivo(string contenedor, string prefijo, TypeMetadato tipometadato)
        {
            ManejadorArchivoS3Response<List<ObjectFile>> archivos = new ManejadorArchivoS3Response<List<ObjectFile>>();
            try
            {

                ListObjectsResponse response = client.ListObjects(contenedor, prefijo);

                if (response.IsTruncated)
                {
                    if (response.S3Objects.Any())
                    {
                        response.S3Objects.ForEach(item =>
                        {
                            ManejadorArchivoS3Response<ObjectFile> objeto = ObtenerArchivo(item.Key, prefijo, item.BucketName, tipometadato);
                            if (objeto.EntidaRespuesta.ContenidoArchivo.Length > 0) archivos.EntidaRespuesta.Add(objeto.EntidaRespuesta);
                        });
                    }
                    archivos.CodigoEstado = response.HttpStatusCode;
                }
            }
            catch (AmazonS3Exception e)
            {
                archivos.MensajeError = e.Message;
                archivos.CodigoEstado = e.StatusCode;
                return archivos;
            }
            return archivos;
        }

    }
}
