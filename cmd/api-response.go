/*
 * MinIO Cloud Storage, (C) 2015, 2016, 2017, 2018 MinIO, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cmd

import (
	"context"
	"encoding/base64"
	"encoding/xml"
	"fmt"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"

	xhttp "storj.io/minio/cmd/http"
	"storj.io/minio/cmd/logger"
	"storj.io/minio/pkg/env"
	"storj.io/minio/pkg/handlers"
	"storj.io/minio/pkg/hash"
)

const (
	// RFC3339 a subset of the ISO8601 timestamp format. e.g 2014-04-29T18:30:38Z
	iso8601TimeFormat = "2006-01-02T15:04:05.000Z" // Reply date format with nanosecond precision.
	maxObjectList     = 1000                       // Limit number of objects in a listObjectsResponse/listObjectsVersionsResponse.
	maxDeleteList     = 10000                      // Limit number of objects deleted in a delete call.
	maxUploadsList    = 10000                      // Limit number of uploads in a listUploadsResponse.
	maxPartsList      = 10000                      // Limit number of parts in a listPartsResponse.
)

// LocationResponse - format for location response.
type LocationResponse struct {
	XMLName  xml.Name `xml:"http://s3.amazonaws.com/doc/2006-03-01/ LocationConstraint" json:"-"`
	Location string   `xml:",chardata"`
}

// PolicyStatus captures information returned by GetBucketPolicyStatusHandler
type PolicyStatus struct {
	XMLName  xml.Name `xml:"http://s3.amazonaws.com/doc/2006-03-01/ PolicyStatus" json:"-"`
	IsPublic string
}

// ListVersionsResponse - format for list bucket versions response.
type ListVersionsResponse struct {
	XMLName xml.Name `xml:"http://s3.amazonaws.com/doc/2006-03-01/ ListVersionsResult" json:"-"`

	Name      string
	Prefix    string
	KeyMarker string

	// When response is truncated (the IsTruncated element value in the response
	// is true), you can use the key name in this field as marker in the subsequent
	// request to get next set of objects. Server lists objects in alphabetical
	// order Note: This element is returned only if you have delimiter request parameter
	// specified. If response does not include the NextMaker and it is truncated,
	// you can use the value of the last Key in the response as the marker in the
	// subsequent request to get the next set of object keys.
	NextKeyMarker string `xml:"NextKeyMarker,omitempty"`

	// When the number of responses exceeds the value of MaxKeys,
	// NextVersionIdMarker specifies the first object version not
	// returned that satisfies the search criteria. Use this value
	// for the version-id-marker request parameter in a subsequent request.
	NextVersionIDMarker string `xml:"NextVersionIdMarker,omitempty"`

	// Marks the last version of the Key returned in a truncated response.
	VersionIDMarker string `xml:"VersionIdMarker"`

	MaxKeys   int
	Delimiter string `xml:"Delimiter,omitempty"`
	// A flag that indicates whether or not ListObjects returned all of the results
	// that satisfied the search criteria.
	IsTruncated bool

	CommonPrefixes []CommonPrefix
	Versions       []ObjectVersion

	// Encoding type used to encode object keys in the response.
	EncodingType string `xml:"EncodingType,omitempty"`
}

// ListObjectsResponse - format for list objects response.
type ListObjectsResponse struct {
	XMLName xml.Name `xml:"http://s3.amazonaws.com/doc/2006-03-01/ ListBucketResult" json:"-"`

	Name   string
	Prefix string
	Marker string

	// When response is truncated (the IsTruncated element value in the response
	// is true), you can use the key name in this field as marker in the subsequent
	// request to get next set of objects. Server lists objects in alphabetical
	// order Note: This element is returned only if you have delimiter request parameter
	// specified. If response does not include the NextMaker and it is truncated,
	// you can use the value of the last Key in the response as the marker in the
	// subsequent request to get the next set of object keys.
	NextMarker string `xml:"NextMarker,omitempty"`

	MaxKeys   int
	Delimiter string `xml:"Delimiter,omitempty"`
	// A flag that indicates whether or not ListObjects returned all of the results
	// that satisfied the search criteria.
	IsTruncated bool

	Contents       []Object
	CommonPrefixes []CommonPrefix

	// Encoding type used to encode object keys in the response.
	EncodingType string `xml:"EncodingType,omitempty"`
}

// ListObjectsV2Response - format for list objects response.
type ListObjectsV2Response struct {
	XMLName xml.Name `xml:"http://s3.amazonaws.com/doc/2006-03-01/ ListBucketResult" json:"-"`

	Name       string
	Prefix     string
	StartAfter string `xml:"StartAfter,omitempty"`
	// When response is truncated (the IsTruncated element value in the response
	// is true), you can use the key name in this field as marker in the subsequent
	// request to get next set of objects. Server lists objects in alphabetical
	// order Note: This element is returned only if you have delimiter request parameter
	// specified. If response does not include the NextMaker and it is truncated,
	// you can use the value of the last Key in the response as the marker in the
	// subsequent request to get the next set of object keys.
	ContinuationToken     string `xml:"ContinuationToken,omitempty"`
	NextContinuationToken string `xml:"NextContinuationToken,omitempty"`

	KeyCount  int
	MaxKeys   int
	Delimiter string `xml:"Delimiter,omitempty"`
	// A flag that indicates whether or not ListObjects returned all of the results
	// that satisfied the search criteria.
	IsTruncated bool

	Contents       []Object
	CommonPrefixes []CommonPrefix

	// Encoding type used to encode object keys in the response.
	EncodingType string `xml:"EncodingType,omitempty"`
}

// Ensure that Part implements xml.Marshaler.
var _ xml.Marshaler = Part{}

// Part container for part metadata.
type Part struct {
	PartNumber   int
	LastModified string
	ETag         string
	Size         int64

	ChecksumAlgorithm hash.Algorithm
	ChecksumValue     string
}

// MarshalXML implements the xml.Marshaler interface.
func (part Part) MarshalXML(e *xml.Encoder, start xml.StartElement) error {
	type simplePart struct {
		PartNumber   int
		LastModified string
		ETag         string
		Size         int64
	}

	type checksum struct {
		XMLName xml.Name
        Value   string `xml:",chardata"`
	}

	completePart := struct {
		simplePart
		Checksum *checksum
	} {
		simplePart: simplePart{
			PartNumber:   part.PartNumber,
			LastModified: part.LastModified,
			ETag:         part.ETag,
			Size:         part.Size,
		},
	}

	if part.ChecksumAlgorithm != hash.AlgorithmNone {
		if !part.ChecksumAlgorithm.IsValid() {
			return fmt.Errorf("invalid checksum algorithm %d", part.ChecksumAlgorithm)
		}

		completePart.Checksum = &checksum{
			XMLName: xml.Name{Local: checksumXMLPrefix+part.ChecksumAlgorithm.String()},
			Value:   part.ChecksumValue,
		}
	}

	return e.EncodeElement(completePart, start)
}

// ListPartsResponse - format for list parts response.
type ListPartsResponse struct {
	XMLName xml.Name `xml:"http://s3.amazonaws.com/doc/2006-03-01/ ListPartsResult" json:"-"`

	Bucket   string
	Key      string
	UploadID string `xml:"UploadId"`

	Initiator Initiator
	Owner     Owner

	// The class of storage used to store the object.
	StorageClass string

	PartNumberMarker     int
	NextPartNumberMarker int
	MaxParts             int
	IsTruncated          bool

	ChecksumAlgorithm string `xml:",omitempty"`
	ChecksumType      string `xml:",omitempty"`

	// List of parts.
	Parts []Part `xml:"Part"`
}

// ListMultipartUploadsResponse - format for list multipart uploads response.
type ListMultipartUploadsResponse struct {
	XMLName xml.Name `xml:"http://s3.amazonaws.com/doc/2006-03-01/ ListMultipartUploadsResult" json:"-"`

	Bucket             string
	KeyMarker          string
	UploadIDMarker     string `xml:"UploadIdMarker"`
	NextKeyMarker      string
	NextUploadIDMarker string `xml:"NextUploadIdMarker"`
	Delimiter          string `xml:"Delimiter,omitempty"`
	Prefix             string
	EncodingType       string `xml:"EncodingType,omitempty"`
	MaxUploads         int
	IsTruncated        bool

	// List of pending uploads.
	Uploads []Upload `xml:"Upload"`

	// Delimed common prefixes.
	CommonPrefixes []CommonPrefix
}

// ListBucketsResponse - format for list buckets response
type ListBucketsResponse struct {
	XMLName xml.Name `xml:"http://s3.amazonaws.com/doc/2006-03-01/ ListAllMyBucketsResult" json:"-"`

	Owner Owner

	// Container for one or more buckets.
	Buckets struct {
		Buckets []Bucket `xml:"Bucket"`
	} // Buckets are nested
}

// Upload container for in progress multipart upload
type Upload struct {
	Key               string
	UploadID          string `xml:"UploadId"`
	Initiator         Initiator
	Owner             Owner
	StorageClass      string
	Initiated         string
	ChecksumAlgorithm string `xml:",omitempty"`
	ChecksumType      string `xml:",omitempty"`
}

// CommonPrefix container for prefix response in ListObjectsResponse
type CommonPrefix struct {
	Prefix string
}

// Bucket container for bucket metadata
type Bucket struct {
	Name         string
	CreationDate string // time string of format "2006-01-02T15:04:05.000Z"
}

// ObjectVersion container for object version metadata
type ObjectVersion struct {
	Object
	IsLatest  bool
	VersionID string `xml:"VersionId"`

	IsDeleteMarker bool
}

// MarshalXML - marshal ObjectVersion
func (o ObjectVersion) MarshalXML(e *xml.Encoder, start xml.StartElement) error {
	if o.IsDeleteMarker {
		start.Name.Local = "DeleteMarker"
	} else {
		start.Name.Local = "Version"
	}

	type objectVersionWrapper ObjectVersion
	return e.EncodeElement(objectVersionWrapper(o), start)
}

// StringMap is a map[string]string.
type StringMap map[string]string

// MarshalXML - StringMap marshals into XML.
func (s StringMap) MarshalXML(e *xml.Encoder, start xml.StartElement) error {

	tokens := []xml.Token{start}

	for key, value := range s {
		t := xml.StartElement{}
		t.Name = xml.Name{
			Space: "",
			Local: key,
		}
		tokens = append(tokens, t, xml.CharData(value), xml.EndElement{Name: t.Name})
	}

	tokens = append(tokens, xml.EndElement{
		Name: start.Name,
	})

	for _, t := range tokens {
		if err := e.EncodeToken(t); err != nil {
			return err
		}
	}

	// flush to ensure tokens are written
	return e.Flush()
}

// Object container for object metadata
type Object struct {
	Key          string
	LastModified string // time string of format "2006-01-02T15:04:05.000Z"
	ETag         string
	Size         int64

	ChecksumAlgorithm string `xml:",omitempty"`
	ChecksumType      string `xml:",omitempty"`

	// Owner of the object.
	Owner Owner

	// The class of storage used to store the object.
	StorageClass string

	// UserMetadata user-defined metadata
	UserMetadata StringMap `xml:"UserMetadata,omitempty"`
}

// ObjectAttributesResponse returns metadata for GetObjectAttributes response.
// TODO: object parts are not supported yet.
type ObjectAttributesResponse struct {
	XMLName      xml.Name                          `xml:"http://s3.amazonaws.com/doc/2006-03-01/ GetObjectAttributesResponse" json:"-"`
	ETag         string                            `xml:"ETag,omitempty"`
	StorageClass string                            `xml:"StorageClass,omitempty"`
	ObjectSize   int64                             `xml:"ObjectSize,omitempty"`
	Checksum     *ObjectAttributesChecksumResponse `xml:"Checksum"`
}

// ObjectAttributesChecksumResponse is an element of ObjectAttributesResponse
// that contains an object's checksum.
type ObjectAttributesChecksumResponse struct {
	Checksum     ChecksumXML
	ChecksumType string `xml:",omitempty"`
}

// ObjectAttributesErrorResponse is a variation of APIErrorResponse that includes
// two additional argument fields specifically for GetObjectAttributes
type ObjectAttributesErrorResponse struct {
	ArgumentName  string `xml:"ArgumentName,omitempty"`
	ArgumentValue string `xml:"ArgumentValue,omitempty"`
	APIErrorResponse
}

// CopyObjectResponse container returns ETag and LastModified of the successfully copied object
type CopyObjectResponse struct {
	XMLName      xml.Name `xml:"http://s3.amazonaws.com/doc/2006-03-01/ CopyObjectResult" json:"-"`
	LastModified string   // time string of format "2006-01-02T15:04:05.000Z"
	ETag         string   // md5sum of the copied object.
	Checksum     ChecksumXML
}

// CopyObjectPartResponse container returns ETag and LastModified of the successfully copied object
type CopyObjectPartResponse struct {
	XMLName      xml.Name `xml:"http://s3.amazonaws.com/doc/2006-03-01/ CopyPartResult" json:"-"`
	LastModified string   // time string of format "2006-01-02T15:04:05.000Z"
	ETag         string   // md5sum of the copied object part.
	Checksum     ChecksumXML
}

// Initiator inherit from Owner struct, fields are same
type Initiator Owner

// Owner - bucket owner/principal
type Owner struct {
	ID          string
	DisplayName string
}

// InitiateMultipartUploadResponse container for InitiateMultiPartUpload response, provides uploadID to start MultiPart upload
type InitiateMultipartUploadResponse struct {
	XMLName xml.Name `xml:"http://s3.amazonaws.com/doc/2006-03-01/ InitiateMultipartUploadResult" json:"-"`

	Bucket   string
	Key      string
	UploadID string `xml:"UploadId"`
}

// CompleteMultipartUploadResponse container for completed multipart upload response
type CompleteMultipartUploadResponse struct {
	XMLName xml.Name `xml:"http://s3.amazonaws.com/doc/2006-03-01/ CompleteMultipartUploadResult" json:"-"`

	Location string
	Bucket   string
	Key      string
	ETag     string

	Checksum     ChecksumXML
	ChecksumType string `xml:",omitempty"`
}

// Ensure that ChecksumXML implements xml.Marshaler.
var _ xml.Marshaler = ChecksumXML{}

// ChecksumXML is a convenience structure for XML-marshalling checksums in the format
// "<Checksum{Algorithm}>value</Checksum{Algorithm}>".
type ChecksumXML struct {
	Algorithm hash.Algorithm
	Value     string
}

// MarshalXML implements the xml.Marshaler interface.
func (c ChecksumXML) MarshalXML(e *xml.Encoder, start xml.StartElement) error {
	if c.Algorithm == hash.AlgorithmNone {
		return nil
	}
	if !c.Algorithm.IsValid() {
		return fmt.Errorf("invalid checksum algorithm %d", c.Algorithm)
	}
	start.Name.Local = checksumXMLPrefix + c.Algorithm.String()
	return e.EncodeElement(c.Value, start)
}

// DeleteError structure.
type DeleteError struct {
	Code      string
	Message   string
	Key       string
	VersionID string `xml:"VersionId"`
}

// DeleteObjectsResponse container for multiple object deletes.
type DeleteObjectsResponse struct {
	XMLName xml.Name `xml:"http://s3.amazonaws.com/doc/2006-03-01/ DeleteResult" json:"-"`

	// Collection of all deleted objects
	DeletedObjects []DeletedObject `xml:"Deleted,omitempty"`

	// Collection of errors deleting certain objects.
	Errors []DeleteError `xml:"Error,omitempty"`
}

// PostResponse container for POST object request when success_action_status is set to 201
type PostResponse struct {
	Bucket   string
	Key      string
	ETag     string
	Location string
}

// returns "https" if the tls boolean is true, "http" otherwise.
func getURLScheme(tls bool) string {
	if tls {
		return httpsScheme
	}
	return httpScheme
}

// getObjectLocation gets the fully qualified URL of an object.
func getObjectLocation(r *http.Request, domains []string, bucket, object string) string {
	// unit tests do not have host set.
	if r.Host == "" {
		return path.Clean(r.URL.Path)
	}
	proto := handlers.GetSourceScheme(r)
	if proto == "" {
		proto = getURLScheme(GlobalIsTLS)
	}
	u := &url.URL{
		Host:   r.Host,
		Path:   path.Join(SlashSeparator, bucket, object),
		Scheme: proto,
	}
	// If domain is set then we need to use bucket DNS style.
	for _, domain := range domains {
		if strings.HasPrefix(r.Host, bucket+"."+domain) {
			u.Path = path.Join(SlashSeparator, object)
			break
		}
	}
	return u.String()
}

// generates ListBucketsResponse from array of BucketInfo which can be
// serialized to match XML and JSON API spec output.
func generateListBucketsResponse(buckets []BucketInfo) ListBucketsResponse {
	listbuckets := make([]Bucket, 0, len(buckets))
	var data = ListBucketsResponse{}
	var owner = Owner{
		ID:          GlobalMinioDefaultOwnerID,
		DisplayName: GlobalMinioDefaultOwnerDisplayName,
	}

	for _, bucket := range buckets {
		var listbucket = Bucket{}
		listbucket.Name = bucket.Name
		listbucket.CreationDate = bucket.Created.UTC().Format(iso8601TimeFormat)
		listbuckets = append(listbuckets, listbucket)
	}

	data.Owner = owner
	data.Buckets.Buckets = listbuckets

	return data
}

// generates an ListBucketVersions response for the said bucket with other enumerated options.
func generateListVersionsResponse(bucket, prefix, marker, versionIDMarker, delimiter, encodingType string, maxKeys int, resp ListObjectVersionsInfo) ListVersionsResponse {
	data := ListVersionsResponse{
		Name:                bucket,
		Versions:            make([]ObjectVersion, 0, len(resp.Objects)),
		EncodingType:        encodingType,
		Prefix:              s3EncodeName(prefix, encodingType),
		KeyMarker:           s3EncodeName(marker, encodingType),
		Delimiter:           s3EncodeName(delimiter, encodingType),
		MaxKeys:             maxKeys,
		NextKeyMarker:       s3EncodeName(resp.NextMarker, encodingType),
		NextVersionIDMarker: resp.NextVersionIDMarker,
		VersionIDMarker:     versionIDMarker,
		IsTruncated:         resp.IsTruncated,
		CommonPrefixes:      make([]CommonPrefix, 0, len(resp.Prefixes)),
	}

	for _, object := range resp.Objects {
		if object.Name == "" {
			continue
		}

		content := ObjectVersion{
			Object: Object{
				Key:          s3EncodeName(object.Name, encodingType),
				LastModified: object.ModTime.UTC().Format(iso8601TimeFormat),
				Size:         object.Size,
				Owner: Owner{
					ID:          GlobalMinioDefaultOwnerID,
					DisplayName: GlobalMinioDefaultOwnerDisplayName,
				},
				ChecksumType: object.ChecksumType.String(),
			},
			VersionID:      object.VersionID,
			IsLatest:       object.IsLatest,
			IsDeleteMarker: object.DeleteMarker,
		}

		if object.ETag != "" {
			content.ETag = "\"" + object.ETag + "\""
		}

		if object.ChecksumAlgorithm != hash.AlgorithmNone {
			content.ChecksumAlgorithm = object.ChecksumAlgorithm.String()
		}

		if object.StorageClass != "" {
			content.StorageClass = object.StorageClass
		} else {
			content.StorageClass = globalMinioDefaultStorageClass
		}

		if content.VersionID == "" {
			content.VersionID = nullVersionID
		}

		data.Versions = append(data.Versions, content)
	}

	for _, prefix := range resp.Prefixes {
		data.CommonPrefixes = append(data.CommonPrefixes, CommonPrefix{
			Prefix: s3EncodeName(prefix, encodingType),
		})
	}

	return data
}

// generates an ListObjectsV1 response for the said bucket with other enumerated options.
func generateListObjectsV1Response(bucket, prefix, marker, delimiter, encodingType string, maxKeys int, resp ListObjectsInfo) ListObjectsResponse {
	data := ListObjectsResponse{
		Name:           bucket,
		Contents:       make([]Object, 0, len(resp.Objects)),
		EncodingType:   encodingType,
		Prefix:         s3EncodeName(prefix, encodingType),
		Marker:         s3EncodeName(marker, encodingType),
		Delimiter:      s3EncodeName(delimiter, encodingType),
		MaxKeys:        maxKeys,
		NextMarker:     s3EncodeName(resp.NextMarker, encodingType),
		IsTruncated:    resp.IsTruncated,
		CommonPrefixes: make([]CommonPrefix, 0, len(resp.Prefixes)),
	}

	for _, object := range resp.Objects {
		if object.Name == "" {
			continue
		}

		content := Object{
			Key:          s3EncodeName(object.Name, encodingType),
			LastModified: object.ModTime.UTC().Format(iso8601TimeFormat),
			Size:         object.Size,
			Owner: Owner{
				ID:          GlobalMinioDefaultOwnerID,
				DisplayName: GlobalMinioDefaultOwnerDisplayName,
			},
			ChecksumType: object.ChecksumType.String(),
		}

		if object.ETag != "" {
			content.ETag = "\"" + object.ETag + "\""
		}

		if object.ChecksumAlgorithm != hash.AlgorithmNone {
			content.ChecksumAlgorithm = object.ChecksumAlgorithm.String()
		}

		if object.StorageClass != "" {
			content.StorageClass = object.StorageClass
		} else {
			content.StorageClass = globalMinioDefaultStorageClass
		}

		data.Contents = append(data.Contents, content)
	}

	for _, prefix := range resp.Prefixes {
		data.CommonPrefixes = append(data.CommonPrefixes, CommonPrefix{
			Prefix: s3EncodeName(prefix, encodingType),
		})
	}

	return data
}

// generates an ListObjectsV2 response for the said bucket with other enumerated options.
func generateListObjectsV2Response(bucket, prefix, token, nextToken, startAfter, delimiter, encodingType string, fetchOwner, isTruncated bool, maxKeys int, objects []ObjectInfo, prefixes []string, metadata bool) ListObjectsV2Response {
	data := ListObjectsV2Response{
		Name:                  bucket,
		Contents:              make([]Object, 0, len(objects)),
		EncodingType:          encodingType,
		StartAfter:            s3EncodeName(startAfter, encodingType),
		Delimiter:             s3EncodeName(delimiter, encodingType),
		Prefix:                s3EncodeName(prefix, encodingType),
		MaxKeys:               maxKeys,
		ContinuationToken:     base64.StdEncoding.EncodeToString([]byte(token)),
		NextContinuationToken: base64.StdEncoding.EncodeToString([]byte(nextToken)),
		IsTruncated:           isTruncated,
		CommonPrefixes:        make([]CommonPrefix, 0, len(prefixes)),
	}

	for _, object := range objects {
		if object.Name == "" {
			continue
		}

		content := Object{
			Key:          s3EncodeName(object.Name, encodingType),
			LastModified: object.ModTime.UTC().Format(iso8601TimeFormat),
			Size:         object.Size,
			Owner: Owner{
				ID:          GlobalMinioDefaultOwnerID,
				DisplayName: GlobalMinioDefaultOwnerDisplayName,
			},
			ChecksumType: object.ChecksumType.String(),
		}

		if object.ETag != "" {
			content.ETag = "\"" + object.ETag + "\""
		}

		if object.ChecksumAlgorithm != hash.AlgorithmNone {
			content.ChecksumAlgorithm = object.ChecksumAlgorithm.String()
		}

		if object.StorageClass != "" {
			content.StorageClass = object.StorageClass
		} else {
			content.StorageClass = globalMinioDefaultStorageClass
		}

		if metadata {
			content.UserMetadata = make(StringMap)
			for k, v := range CleanMinioInternalMetadataKeys(object.UserDefined) {
				if strings.HasPrefix(strings.ToLower(k), ReservedMetadataPrefixLower) {
					// Do not need to send any internal metadata
					// values to client.
					continue
				}
				// https://github.com/google/security-research/security/advisories/GHSA-76wf-9vgp-pj7w
				if equals(k, xhttp.AmzMetaUnencryptedContentLength, xhttp.AmzMetaUnencryptedContentMD5) {
					continue
				}
				content.UserMetadata[k] = v
			}
		}

		data.Contents = append(data.Contents, content)
	}

	for _, prefix := range prefixes {
		data.CommonPrefixes = append(data.CommonPrefixes, CommonPrefix{
			Prefix: s3EncodeName(prefix, encodingType),
		})
	}

	data.KeyCount = len(data.Contents) + len(data.CommonPrefixes)

	return data
}

// generates CopyObjectResponse from etag and lastModified time.
func generateCopyObjectResponse(objInfo ObjectInfo) CopyObjectResponse {
	return CopyObjectResponse{
		ETag:         "\"" + objInfo.ETag + "\"",
		LastModified: objInfo.ModTime.UTC().Format(iso8601TimeFormat),
		Checksum: ChecksumXML{
			Algorithm: objInfo.ChecksumAlgorithm,
			Value:     objInfo.ChecksumValue,
		},
	}
}

// generates CopyObjectPartResponse from etag and lastModified time.
func generateCopyObjectPartResponse(partInfo PartInfo) CopyObjectPartResponse {
	return CopyObjectPartResponse{
		ETag:         "\"" + partInfo.ETag + "\"",
		LastModified: partInfo.LastModified.UTC().Format(iso8601TimeFormat),
		Checksum: ChecksumXML{
			Algorithm: partInfo.ChecksumAlgorithm,
			Value:     partInfo.ChecksumValue,
		},
	}
}

// generates InitiateMultipartUploadResponse for given bucket, key and uploadID.
func generateInitiateMultipartUploadResponse(info MultipartInfo) InitiateMultipartUploadResponse {
	return InitiateMultipartUploadResponse{
		Bucket:   info.Bucket,
		Key:      info.Object,
		UploadID: info.UploadID,
	}
}

// generates CompleteMultipartUploadResponse for given bucket, key, location and ETag.
func generateCompleteMultpartUploadResponse(objInfo ObjectInfo, location string) CompleteMultipartUploadResponse {
	return CompleteMultipartUploadResponse{
		Location: location,
		Bucket:   objInfo.Bucket,
		Key:      objInfo.Name,
		// AWS S3 quotes the ETag in XML, make sure we are compatible here.
		ETag: "\"" + objInfo.ETag + "\"",
		Checksum: ChecksumXML{
			Algorithm: objInfo.ChecksumAlgorithm,
			Value:     objInfo.ChecksumValue,
		},
		ChecksumType: objInfo.ChecksumType.String(),
	}
}

// generates ListPartsResponse from ListPartsInfo.
func generateListPartsResponse(partsInfo ListPartsInfo, encodingType string) ListPartsResponse {
	listPartsResponse := ListPartsResponse{
		Bucket:               partsInfo.Bucket,
		Key:                  s3EncodeName(partsInfo.Object, encodingType),
		UploadID:             partsInfo.UploadID,
		StorageClass:         globalMinioDefaultStorageClass,
		MaxParts:             partsInfo.MaxParts,
		PartNumberMarker:     partsInfo.PartNumberMarker,
		IsTruncated:          partsInfo.IsTruncated,
		NextPartNumberMarker: partsInfo.NextPartNumberMarker,
		Parts:                make([]Part, len(partsInfo.Parts)),
		ChecksumType:         partsInfo.ChecksumType.String(),

		// Dumb values not meaningful
		Initiator: Initiator{
			ID:          GlobalMinioDefaultOwnerID,
			DisplayName: GlobalMinioDefaultOwnerID,
		},
		Owner: Owner{
			ID:          GlobalMinioDefaultOwnerID,
			DisplayName: GlobalMinioDefaultOwnerID,
		},
	}

	if partsInfo.ChecksumAlgorithm != hash.AlgorithmNone {
		listPartsResponse.ChecksumAlgorithm = partsInfo.ChecksumAlgorithm.String()
	}

	for index, part := range partsInfo.Parts {
		listPartsResponse.Parts[index] = Part{
			PartNumber:        part.PartNumber,
			ETag:              "\"" + part.ETag + "\"",
			Size:              part.Size,
			LastModified:      part.LastModified.UTC().Format(iso8601TimeFormat),
			ChecksumAlgorithm: part.ChecksumAlgorithm,
			ChecksumValue:     part.ChecksumValue,
		}
	}
	return listPartsResponse
}

// generates ListMultipartUploadsResponse for given bucket and ListMultipartsInfo.
func generateListMultipartUploadsResponse(bucket string, multipartsInfo ListMultipartsInfo, encodingType string) ListMultipartUploadsResponse {
	listMultipartUploadsResponse := ListMultipartUploadsResponse{
		Bucket:             bucket,
		Delimiter:          s3EncodeName(multipartsInfo.Delimiter, encodingType),
		IsTruncated:        multipartsInfo.IsTruncated,
		EncodingType:       encodingType,
		Prefix:             s3EncodeName(multipartsInfo.Prefix, encodingType),
		KeyMarker:          s3EncodeName(multipartsInfo.KeyMarker, encodingType),
		NextKeyMarker:      s3EncodeName(multipartsInfo.NextKeyMarker, encodingType),
		MaxUploads:         multipartsInfo.MaxUploads,
		NextUploadIDMarker: multipartsInfo.NextUploadIDMarker,
		UploadIDMarker:     multipartsInfo.UploadIDMarker,
		CommonPrefixes:     make([]CommonPrefix, 0, len(multipartsInfo.CommonPrefixes)),
		Uploads:            make([]Upload, 0, len(multipartsInfo.Uploads)),
	}

	for _, commonPrefix := range multipartsInfo.CommonPrefixes {
		listMultipartUploadsResponse.CommonPrefixes = append(listMultipartUploadsResponse.CommonPrefixes, CommonPrefix{
			Prefix: s3EncodeName(commonPrefix, encodingType),
		})
	}

	for _, info := range multipartsInfo.Uploads {
		upload := Upload{
			UploadID:     info.UploadID,
			Key:          s3EncodeName(info.Object, encodingType),
			Initiated:    info.Initiated.UTC().Format(iso8601TimeFormat),
			ChecksumType: info.ChecksumType.String(),
		}

		if info.ChecksumAlgorithm != hash.AlgorithmNone {
			upload.ChecksumAlgorithm = info.ChecksumAlgorithm.String()
		}

		listMultipartUploadsResponse.Uploads = append(listMultipartUploadsResponse.Uploads, upload)
	}

	return listMultipartUploadsResponse
}

// generate multi objects delete response.
func generateMultiDeleteResponse(quiet bool, deletedObjects []DeletedObject, errs []DeleteError) DeleteObjectsResponse {
	deleteResp := DeleteObjectsResponse{}
	if !quiet {
		deleteResp.DeletedObjects = deletedObjects
	}
	deleteResp.Errors = errs
	return deleteResp
}

func writeResponse(w http.ResponseWriter, statusCode int, response []byte, mType mimeType) {
	setCommonHeaders(w)
	if mType != mimeNone {
		w.Header().Set(xhttp.ContentType, string(mType))
	}
	w.Header().Set(xhttp.ContentLength, strconv.Itoa(len(response)))
	w.WriteHeader(statusCode)
	if response != nil {
		w.Write(response)
		w.(http.Flusher).Flush()
	}
}

// mimeType represents various MIME type used API responses.
type mimeType string

const (
	// Means no response type.
	mimeNone mimeType = ""
	// Means response type is JSON.
	mimeJSON mimeType = "application/json"
	// Means response type is XML.
	mimeXML mimeType = "application/xml"
)

// writeSuccessResponseJSON writes success headers and response if any,
// with content-type set to `application/json`.
func writeSuccessResponseJSON(w http.ResponseWriter, response []byte) {
	writeResponse(w, http.StatusOK, response, mimeJSON)
}

// WriteSuccessResponseXML writes success headers and response if any,
// with content-type set to `application/xml`.
func WriteSuccessResponseXML(w http.ResponseWriter, response []byte) {
	writeResponse(w, http.StatusOK, response, mimeXML)
}

// writeSuccessNoContent writes success headers with http status 204
func writeSuccessNoContent(w http.ResponseWriter) {
	writeResponse(w, http.StatusNoContent, nil, mimeNone)
}

// writeRedirectSeeOther writes Location header with http status 303
func writeRedirectSeeOther(w http.ResponseWriter, location string) {
	w.Header().Set(xhttp.Location, location)
	writeResponse(w, http.StatusSeeOther, nil, mimeNone)
}

func writeSuccessResponseHeadersOnly(w http.ResponseWriter) {
	writeResponse(w, http.StatusOK, nil, mimeNone)
}

var storjRetryAfter = env.Get("STORJ_MINIO_RETRY_AFTER", "12")

// writeErrorRespone writes error headers
func WriteErrorResponse(ctx context.Context, w http.ResponseWriter, apiErr APIError, reqURL *url.URL, browser bool) {
	switch apiErr.Code {
	case "SlowDown", "XMinioServerNotInitialized", "XMinioReadQuorum", "XMinioWriteQuorum":
		// Set retry-after header to indicate user-agents to retry request after 120secs.
		// https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Retry-After
		w.Header().Set(xhttp.RetryAfter, storjRetryAfter)
	case "InvalidRegion":
		apiErr.Description = fmt.Sprintf("Region does not match; expecting '%s'.", globalServerRegion)
	case "AuthorizationHeaderMalformed":
		apiErr.Description = fmt.Sprintf("The authorization header is malformed; the region is wrong; expecting '%s'.", globalServerRegion)
	case "AccessDenied":
		// The request is from browser and also if browser
		// is enabled we need to redirect.
		if browser && globalBrowserEnabled {
			w.Header().Set(xhttp.Location, minioReservedBucketPath+reqURL.Path)
			w.WriteHeader(http.StatusTemporaryRedirect)
			return
		}
	}

	// Generate error response.
	errorResponse := getAPIErrorResponse(ctx, apiErr, reqURL.Path,
		w.Header().Get(xhttp.AmzRequestID), globalDeploymentID)

	encodedErrorResponse, err := EncodeResponse(errorResponse)
	if err != nil {
		logger.LogIf(ctx, fmt.Errorf("error encoding XML error response: %w", err))
		writeResponse(w, http.StatusInternalServerError, nil, mimeNone)
		return
	}

	writeResponse(w, apiErr.HTTPStatusCode, encodedErrorResponse, mimeXML)
}

func writeErrorResponseHeadersOnly(w http.ResponseWriter, err APIError) {
	writeResponse(w, err.HTTPStatusCode, nil, mimeNone)
}

func writeErrorResponseString(ctx context.Context, w http.ResponseWriter, err APIError, reqURL *url.URL) {
	// Generate string error response.
	writeResponse(w, err.HTTPStatusCode, []byte(err.Description), mimeNone)
}

// writeErrorResponseJSON - writes error response in JSON format;
// useful for admin APIs.
func writeErrorResponseJSON(ctx context.Context, w http.ResponseWriter, err APIError, reqURL *url.URL) {
	// Generate error response.
	errorResponse := getAPIErrorResponse(ctx, err, reqURL.Path, w.Header().Get(xhttp.AmzRequestID), globalDeploymentID)
	encodedErrorResponse := encodeResponseJSON(errorResponse)
	writeResponse(w, err.HTTPStatusCode, encodedErrorResponse, mimeJSON)
}

// writeCustomErrorResponseJSON - similar to writeErrorResponseJSON,
// but accepts the error message directly (this allows messages to be
// dynamically generated.)
func writeCustomErrorResponseJSON(ctx context.Context, w http.ResponseWriter, err APIError,
	errBody string, reqURL *url.URL) {

	reqInfo := logger.GetReqInfo(ctx)
	errorResponse := APIErrorResponse{
		Code:       err.Code,
		Message:    errBody,
		Resource:   reqURL.Path,
		BucketName: reqInfo.BucketName,
		Key:        reqInfo.ObjectName,
		RequestID:  w.Header().Get(xhttp.AmzRequestID),
		HostID:     globalDeploymentID,
	}
	encodedErrorResponse := encodeResponseJSON(errorResponse)
	writeResponse(w, err.HTTPStatusCode, encodedErrorResponse, mimeJSON)
}
